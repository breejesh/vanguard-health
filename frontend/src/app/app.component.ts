import { AfterViewInit, Component } from '@angular/core';
import * as L from 'leaflet';
import * as H3 from 'h3-js';
import { initializeApp } from 'firebase/app';
import { collection, doc, Firestore, getDoc, getDocs, getFirestore, orderBy, query, where } from 'firebase/firestore';
import { environment } from '../environments/environment';

interface HotspotPoint {
  lat: number;
  lon: number;
  h3: string;
  conditionCode: string;
  conditionDisplay: string;
  cases: number;
  latestDate: string;
  mergedCount?: number;
}

interface ConditionOption {
  code: string;
  display: string;
  patientCount: number;
}

interface DatabaseOverviewStats {
  totalConditions: number;
  totalPatients: number;
  totalCases: number;
  totalCells: number;
}

interface FilteredStats {
  totalCases: number;
  cellCount: number;
  activeDays: number;
}

interface UsStatesFeatureCollection {
  type: 'FeatureCollection';
  features: UsStateFeature[];
}

interface UsStateFeature {
  type: 'Feature';
  properties: {
    name?: string;
  };
  geometry: {
    type: 'Polygon' | 'MultiPolygon';
    coordinates: number[][][] | number[][][][];
  };
}

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent implements AfterViewInit {
  private readonly usBounds: L.LatLngBoundsExpression = [
    [24.0, -125.0],
    [49.8, -66.0],
  ];
  private readonly showStatePolygonsByDefault = false;
  private readonly showStateLabelsByDefault = false;

  private map?: L.Map;
  private hotspotLayer?: L.LayerGroup;
  private cityRiskLayer?: L.LayerGroup;
  private stateBoundaryLayer?: L.LayerGroup;
  private stateLabelLayer?: L.LayerGroup;
  private db?: Firestore;
  private usStatesGeoJson?: UsStatesFeatureCollection;

  conditionOptions: ConditionOption[] = [];
  readonly timeWindowOptions = [
    { label: '1D', days: 1 },
    { label: '7D', days: 7 },
    { label: '30D', days: 30 },
    { label: '90D', days: 90 },
    { label: '180D', days: 180 },
  ];

  selectedCondition = '';
  selectedWindowDays = 30;
  customWindowDays = 30;
  lastUpdatedLabel = 'Not synced yet';
  loading = false;
  errorMessage = '';

  private hotspots: HotspotPoint[] = [];

  totalStats: DatabaseOverviewStats = {
    totalConditions: 0,
    totalPatients: 0,
    totalCases: 0,
    totalCells: 0,
  };

  filteredStats: FilteredStats = {
    totalCases: 0,
    cellCount: 0,
    activeDays: 0,
  };

  async ngAfterViewInit(): Promise<void> {
    this.initFirestore();
    this.initMap();
    await this.loadOverviewAndConditions();
    await this.refreshForCurrentFilters();
  }

  async onConditionChange(): Promise<void> {
    await this.refreshForCurrentFilters();
  }

  async setTimeWindow(days: number): Promise<void> {
    this.selectedWindowDays = days;
    this.customWindowDays = days;
    await this.refreshForCurrentFilters();
  }

  async normalizeCustomWindow(): Promise<void> {
    const bounded = Math.max(1, Math.min(180, Number(this.customWindowDays) || 30));
    this.selectedWindowDays = bounded;
    this.customWindowDays = bounded;
    await this.refreshForCurrentFilters();
  }

  private initFirestore(): void {
    try {
      if (!environment.firebase?.projectId) {
        this.errorMessage = 'Firebase projectId is missing in frontend/src/environments/environment*.ts';
        return;
      }

      const app = initializeApp(environment.firebase);
      this.db = getFirestore(app);
    } catch (err) {
      this.errorMessage = `Failed to initialize Firebase: ${String(err)}`;
    }
  }

  private async loadOverviewAndConditions(): Promise<void> {
    if (!this.db) {
      return;
    }

    this.loading = true;
    this.errorMessage = '';
    try {
      const overviewSnap = await getDoc(doc(this.db, 'gold_overview', 'current'));
      if (overviewSnap.exists()) {
        const payload = overviewSnap.data() as Record<string, unknown>;
        this.totalStats.totalConditions = Number(payload['total_conditions'] ?? 0);
        this.totalStats.totalCases = Number(payload['total_cases'] ?? 0);
        this.totalStats.totalCells = Number(payload['total_h3_cells'] ?? 0);

        const firebaseTotal = Number(
          payload['total_patients'] ?? payload['total_cases_overall'] ?? payload['total_cases'] ?? 0
        );
        this.totalStats.totalPatients = firebaseTotal;

        const lastUpdateRaw =
          payload['last_update_time'] ?? payload['generated_at'] ?? payload['updated_at'] ?? null;
        if (lastUpdateRaw) {
          const parsed = new Date(String(lastUpdateRaw));
          this.lastUpdatedLabel = Number.isNaN(parsed.getTime()) ? String(lastUpdateRaw) : parsed.toLocaleString();
        }
      }

      const conditionQuery = query(collection(this.db, 'gold_conditions'), orderBy('display_name'));
      const conditionSnap = await getDocs(conditionQuery);

      this.conditionOptions = conditionSnap.docs.map((d) => {
        const data = d.data() as Record<string, unknown>;
        return {
          code: String(data['condition_code'] ?? d.id),
          display: String(data['display_name'] ?? data['condition_code'] ?? d.id),
          patientCount: Number(data['patient_count'] ?? 0),
        };
      });

      if (!this.totalStats.totalPatients) {
        this.totalStats.totalPatients = this.conditionOptions.reduce((sum, option) => sum + option.patientCount, 0);
      }
      if (this.totalStats.totalConditions === 0) {
        this.totalStats.totalConditions = this.conditionOptions.length;
      }

      if (!this.selectedCondition && this.conditionOptions.length > 0) {
        this.selectedCondition = this.conditionOptions[0].code;
      }
    } catch (err) {
      this.errorMessage = `Failed to load Firestore metadata: ${String(err)}`;
    } finally {
      this.loading = false;
    }
  }

  private async refreshForCurrentFilters(): Promise<void> {
    if (!this.db || !this.selectedCondition) {
      this.hotspots = [];
      this.filteredStats = { totalCases: 0, cellCount: 0, activeDays: 0 };
      this.renderHotspots();
      return;
    }

    this.loading = true;
    this.errorMessage = '';
    try {
      const endDateKey = this.dateKeyFromNow(0);
      const startDateKey = this.dateKeyFromNow(-(this.selectedWindowDays - 1));

      const cellsQuery = query(
        collection(this.db, 'gold_daily_cells'),
        where('condition_code', '==', this.selectedCondition),
        where('date_key', '>=', startDateKey),
        where('date_key', '<=', endDateKey),
      );
      const snap = await getDocs(cellsQuery);

      const byH3 = new Map<string, { caseCount: number; latestDate: string }>();
      const activeDays = new Set<string>();
      let totalCases = 0;

      for (const d of snap.docs) {
        const row = d.data() as Record<string, unknown>;
        const h3 = String(row['h3'] ?? '');
        const dateKey = String(row['date_key'] ?? '');
        const caseCount = Number(row['case_count'] ?? 0);
        if (!h3 || !dateKey || caseCount <= 0) {
          continue;
        }

        activeDays.add(dateKey);
        totalCases += caseCount;

        const existing = byH3.get(h3);
        if (!existing) {
          byH3.set(h3, { caseCount, latestDate: dateKey });
        } else {
          existing.caseCount += caseCount;
          if (dateKey > existing.latestDate) {
            existing.latestDate = dateKey;
          }
        }
      }

      const selectedConditionDisplay = this.conditionOptions.find((c) => c.code === this.selectedCondition)?.display ?? this.selectedCondition;
      const points: HotspotPoint[] = [];
      for (const [h3, agg] of byH3.entries()) {
        const [lat, lon] = this.h3ToLatLon(h3);
        points.push({
          lat,
          lon,
          h3,
          conditionCode: this.selectedCondition,
          conditionDisplay: selectedConditionDisplay,
          cases: agg.caseCount,
          latestDate: agg.latestDate,
        });
      }

      this.hotspots = points;
      this.filteredStats = {
        totalCases,
        cellCount: points.length,
        activeDays: activeDays.size,
      };
      this.lastUpdatedLabel = new Date().toLocaleString();
      this.renderHotspots();
    } catch (err) {
      this.errorMessage = `Failed to query Firestore daily cells: ${String(err)}`;
      this.hotspots = [];
      this.filteredStats = { totalCases: 0, cellCount: 0, activeDays: 0 };
      this.renderHotspots();
    } finally {
      this.loading = false;
    }
  }

  private initMap(): void {
    this.map = L.map('health-map', {
      zoomControl: true,
      attributionControl: true,
      maxBounds: this.usBounds,
      maxBoundsViscosity: 1.0,
      minZoom: 3,
    }).setView([39.5, -98.35], 4);

    L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
      maxZoom: 12,
    }).addTo(this.map);

    this.map.createPane('statePane');
    this.map.getPane('statePane')!.style.zIndex = '300';

    this.map.createPane('riskPane');
    this.map.getPane('riskPane')!.style.zIndex = '420';

    this.map.createPane('hotspotPane');
    this.map.getPane('hotspotPane')!.style.zIndex = '520';

    this.map.createPane('labelPane');
    this.map.getPane('labelPane')!.style.zIndex = '650';

    this.hotspotLayer = L.layerGroup().addTo(this.map);
    this.cityRiskLayer = L.layerGroup().addTo(this.map);
    this.stateBoundaryLayer = L.layerGroup().addTo(this.map);
    this.stateLabelLayer = L.layerGroup().addTo(this.map);
  }

  private renderHotspots(): void {
    if (!this.map || !this.hotspotLayer) {
      return;
    }

    this.hotspotLayer.clearLayers();
    this.cityRiskLayer?.clearLayers();
    this.stateBoundaryLayer?.clearLayers();
    this.stateLabelLayer?.clearLayers();
    const points = this.mergeCoincidentHotspots(this.hotspots);
    const maxCases = Math.max(...points.map((p) => p.cases), 1);

    for (const point of points) {
      const intensity = Math.min(1, point.cases / maxCases);
      const fillColor = this.pickHeatColor(intensity);
      const baseRadiusMeters = 12000 + intensity * 18000;

      L.circle([point.lat, point.lon], {
        pane: 'hotspotPane',
        radius: baseRadiusMeters * 1.85,
        color: fillColor,
        weight: 1.2,
        dashArray: '6 6',
        fillColor,
        fillOpacity: 0.08,
      }).addTo(this.hotspotLayer);

      L.circle([point.lat, point.lon], {
        pane: 'hotspotPane',
        radius: baseRadiusMeters,
        color: fillColor,
        weight: 1.2,
        fillColor,
        fillOpacity: 0.3,
      })
        .bindPopup(
          `<strong>${point.conditionDisplay}</strong><br/>Cases: ${point.cases}<br/>Latest date: ${point.latestDate}${point.mergedCount && point.mergedCount > 1 ? `<br/>Merged hotspots: ${point.mergedCount}` : ''}`
        )
        .addTo(this.hotspotLayer);
    }

    if (this.showStatePolygonsByDefault) {
      void this.renderStateBoundaries();
    }

    if (points.length > 0) {
      const bounds = L.latLngBounds(points.map((p) => [p.lat, p.lon] as [number, number]));
      this.map.fitBounds(bounds.pad(0.35));
      this.map.panInsideBounds(this.usBounds, { animate: false });
    }

    if (this.showStateLabelsByDefault) {
      void this.renderStateLabels(points);
    }
  }

  private mergeCoincidentHotspots(points: HotspotPoint[]): HotspotPoint[] {
    const merged = new Map<string, HotspotPoint>();

    for (const point of points) {
      const key = `${point.lat.toFixed(6)}|${point.lon.toFixed(6)}`;
      const existing = merged.get(key);

      if (!existing) {
        merged.set(key, { ...point, mergedCount: 1 });
        continue;
      }

      existing.cases += point.cases;
      existing.mergedCount = (existing.mergedCount ?? 1) + 1;
      if (point.latestDate > existing.latestDate) {
        existing.latestDate = point.latestDate;
      }
    }

    return Array.from(merged.values());
  }

  private async renderStateBoundaries(): Promise<void> {
    if (!this.stateBoundaryLayer) {
      return;
    }

    await this.ensureUsStatesGeoJson();
    if (!this.usStatesGeoJson) {
      return;
    }

    for (const feature of this.usStatesGeoJson.features) {
      if (feature.geometry.type === 'Polygon') {
        const polygonLatLngs = this.polygonToLatLngs(feature.geometry.coordinates as number[][][]);
        L.polygon(polygonLatLngs, {
          pane: 'statePane',
          color: '#5c9dc2',
          weight: 1,
          fillColor: '#ffffff',
          fillOpacity: 0.9,
          interactive: false,
        }).addTo(this.stateBoundaryLayer);
        continue;
      }

      const multiPolygonLatLngs = (feature.geometry.coordinates as number[][][][]).map((polygon) =>
        this.polygonToLatLngs(polygon)
      );
      L.polygon(multiPolygonLatLngs, {
        pane: 'statePane',
        color: '#5c9dc2',
        weight: 1,
        fillColor: '#ffffff',
        fillOpacity: 0.9,
        interactive: false,
      }).addTo(this.stateBoundaryLayer);
    }
  }

  private polygonToLatLngs(polygon: number[][][]): L.LatLngExpression[][] {
    return polygon.map((ring) => ring.map(([lon, lat]) => [lat, lon] as [number, number]));
  }

  private h3BoundaryToLatLngs(h3Id: string): L.LatLngExpression[] {
    const anyH3 = H3 as unknown as {
      cellToBoundary?: (id: string, formatAsGeoJson?: boolean) => Array<[number, number]>;
      h3ToGeoBoundary?: (id: string, formatAsGeoJson?: boolean) => Array<[number, number]>;
    };

    if (typeof anyH3.cellToBoundary === 'function') {
      const boundary = anyH3.cellToBoundary(h3Id, false);
      return boundary.map(([lat, lon]) => [lat, lon] as [number, number]);
    }

    if (typeof anyH3.h3ToGeoBoundary === 'function') {
      const boundary = anyH3.h3ToGeoBoundary(h3Id, false);
      return boundary.map(([lat, lon]) => [lat, lon] as [number, number]);
    }

    throw new Error('No supported H3 boundary method found in h3-js');
  }

  private getH3Resolution(h3Id: string): number {
    const anyH3 = H3 as unknown as {
      getResolution?: (id: string) => number;
      h3GetResolution?: (id: string) => number;
    };

    if (typeof anyH3.getResolution === 'function') {
      return anyH3.getResolution(h3Id);
    }

    if (typeof anyH3.h3GetResolution === 'function') {
      return anyH3.h3GetResolution(h3Id);
    }

    return 0;
  }

  private expandHotspotsToResolution(points: HotspotPoint[], targetResolution: number): HotspotPoint[] {
    const anyH3 = H3 as unknown as {
      cellToChildren?: (id: string, res: number) => string[];
      h3ToChildren?: (id: string, res: number) => string[];
      cellToParent?: (id: string, res: number) => string;
      h3ToParent?: (id: string, res: number) => string;
    };

    const byCell = new Map<string, HotspotPoint>();

    for (const point of points) {
      const sourceResolution = this.getH3Resolution(point.h3);
      let targetCells: string[] = [point.h3];

      if (sourceResolution < targetResolution) {
        if (typeof anyH3.cellToChildren === 'function') {
          targetCells = anyH3.cellToChildren(point.h3, targetResolution);
        } else if (typeof anyH3.h3ToChildren === 'function') {
          targetCells = anyH3.h3ToChildren(point.h3, targetResolution);
        }
      } else if (sourceResolution > targetResolution) {
        let parentCell = point.h3;
        if (typeof anyH3.cellToParent === 'function') {
          parentCell = anyH3.cellToParent(point.h3, targetResolution);
        } else if (typeof anyH3.h3ToParent === 'function') {
          parentCell = anyH3.h3ToParent(point.h3, targetResolution);
        }
        targetCells = [parentCell];
      }

      const perCellCases = point.cases / Math.max(1, targetCells.length);
      for (const cellId of targetCells) {
        const existing = byCell.get(cellId);
        if (!existing) {
          const [lat, lon] = this.h3ToLatLon(cellId);
          byCell.set(cellId, {
            ...point,
            h3: cellId,
            lat,
            lon,
            cases: perCellCases,
          });
        } else {
          existing.cases += perCellCases;
          if (point.latestDate > existing.latestDate) {
            existing.latestDate = point.latestDate;
          }
        }
      }
    }

    return Array.from(byCell.values());
  }

  private scaleHexAroundCenter(points: L.LatLngExpression[], scale: number): L.LatLngExpression[] {
    const latLngPairs = points.map((p) => p as [number, number]);
    const center = this.computePolygonCenter(latLngPairs);

    return latLngPairs.map(([lat, lon]) => {
      const scaledLat = center.lat + (lat - center.lat) * scale;
      const scaledLon = center.lon + (lon - center.lon) * scale;
      return [scaledLat, scaledLon] as [number, number];
    });
  }

  private computePolygonCenter(points: [number, number][]): { lat: number; lon: number } {
    if (points.length === 0) {
      return { lat: 0, lon: 0 };
    }

    let latSum = 0;
    let lonSum = 0;
    for (const [lat, lon] of points) {
      latSum += lat;
      lonSum += lon;
    }

    return {
      lat: latSum / points.length,
      lon: lonSum / points.length,
    };
  }

  private renderAtRiskCityBorders(points: HotspotPoint[]): void {
    if (!this.cityRiskLayer || points.length === 0) {
      return;
    }

    const clusters = this.clusterHotspots(points, 70);
    const maxClusterCases = Math.max(...clusters.map((cluster) => cluster.totalCases), 1);

    for (const cluster of clusters) {
      const intensity = Math.min(1, cluster.totalCases / maxClusterCases);
      const borderColor = this.pickHeatColor(intensity);
      const borderWeight = 1.5 + intensity * 2.5;

      if (cluster.points.length === 1) {
        const point = cluster.points[0];
        const radiusMeters = 22000 + Math.min(point.cases, 300) * 90;
        L.circle([point.lat, point.lon], {
          pane: 'riskPane',
          radius: radiusMeters,
          color: borderColor,
          fillColor: borderColor,
          fillOpacity: 0.1,
          weight: borderWeight,
          dashArray: '6 6',
        })
          .bindTooltip(`At-risk zone<br/>Cases: ${cluster.totalCases}<br/>Hotspot cells: ${cluster.points.length}`)
          .addTo(this.cityRiskLayer);
        continue;
      }

      const hull = this.computeConvexHull(cluster.points.map((p) => [p.lon, p.lat] as [number, number]));
      if (hull.length < 3) {
        continue;
      }

      L.polygon(
        hull.map(([lon, lat]) => [lat, lon] as [number, number]),
        {
          pane: 'riskPane',
          color: borderColor,
          fillColor: borderColor,
          fillOpacity: 0.1,
          weight: borderWeight,
          dashArray: '8 6',
        }
      )
        .bindTooltip(`At-risk zone<br/>Cases: ${cluster.totalCases}<br/>Hotspot cells: ${cluster.points.length}`)
        .addTo(this.cityRiskLayer);
    }
  }

  private clusterHotspots(points: HotspotPoint[], maxDistanceKm: number): Array<{ points: HotspotPoint[]; totalCases: number }> {
    const clusters: Array<{ points: HotspotPoint[]; totalCases: number }> = [];

    for (const point of points) {
      let bestClusterIndex = -1;
      let bestDistance = Number.POSITIVE_INFINITY;

      for (let i = 0; i < clusters.length; i += 1) {
        const centroid = this.clusterCentroid(clusters[i].points);
        const distance = this.haversineKm(point.lat, point.lon, centroid.lat, centroid.lon);
        if (distance <= maxDistanceKm && distance < bestDistance) {
          bestDistance = distance;
          bestClusterIndex = i;
        }
      }

      if (bestClusterIndex >= 0) {
        clusters[bestClusterIndex].points.push(point);
        clusters[bestClusterIndex].totalCases += point.cases;
      } else {
        clusters.push({ points: [point], totalCases: point.cases });
      }
    }

    return clusters;
  }

  private clusterCentroid(points: HotspotPoint[]): { lat: number; lon: number } {
    if (points.length === 0) {
      return { lat: 0, lon: 0 };
    }

    let latSum = 0;
    let lonSum = 0;
    for (const point of points) {
      latSum += point.lat;
      lonSum += point.lon;
    }

    return {
      lat: latSum / points.length,
      lon: lonSum / points.length,
    };
  }

  private haversineKm(lat1: number, lon1: number, lat2: number, lon2: number): number {
    const toRad = (value: number) => (value * Math.PI) / 180;
    const dLat = toRad(lat2 - lat1);
    const dLon = toRad(lon2 - lon1);
    const a =
      Math.sin(dLat / 2) ** 2 +
      Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
    return 6371 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  }

  private computeConvexHull(points: [number, number][]): [number, number][] {
    if (points.length <= 3) {
      return points;
    }

    const sorted = [...points].sort((a, b) => (a[0] === b[0] ? a[1] - b[1] : a[0] - b[0]));
    const lower: [number, number][] = [];
    const upper: [number, number][] = [];

    for (const point of sorted) {
      while (lower.length >= 2 && this.cross(lower[lower.length - 2], lower[lower.length - 1], point) <= 0) {
        lower.pop();
      }
      lower.push(point);
    }

    for (let i = sorted.length - 1; i >= 0; i -= 1) {
      const point = sorted[i];
      while (upper.length >= 2 && this.cross(upper[upper.length - 2], upper[upper.length - 1], point) <= 0) {
        upper.pop();
      }
      upper.push(point);
    }

    lower.pop();
    upper.pop();
    return [...lower, ...upper];
  }

  private cross(o: [number, number], a: [number, number], b: [number, number]): number {
    return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0]);
  }

  private async renderStateLabels(points: HotspotPoint[]): Promise<void> {
    if (!this.map || !this.stateLabelLayer) {
      return;
    }

    await this.ensureUsStatesGeoJson();
    if (!this.usStatesGeoJson) {
      return;
    }

    const stateCaseTotals = new Map<string, number>();
    for (const point of points) {
      const stateName = this.findStateForPoint(point.lon, point.lat);
      if (stateName) {
        stateCaseTotals.set(stateName, (stateCaseTotals.get(stateName) ?? 0) + point.cases);
      }
    }

    const maxStateCases = Math.max(...Array.from(stateCaseTotals.values()), 1);

    for (const feature of this.usStatesGeoJson.features) {
      const stateName = feature.properties?.name;
      if (!stateName) {
        continue;
      }

      const stateCases = stateCaseTotals.get(stateName) ?? 0;
      const labelStyle = this.stateLabelStyle(stateCases, maxStateCases);

      const [lat, lon] = this.featureCenter(feature);
      L.marker([lat, lon], {
        pane: 'labelPane',
        icon: L.divIcon({
          className: 'state-label-marker',
          html: `<span class="state-label-chip" style="${labelStyle}">${stateName}</span>`,
          iconSize: [0, 0],
        }),
        interactive: false,
        keyboard: false,
      }).addTo(this.stateLabelLayer);
    }
  }

  private async ensureUsStatesGeoJson(): Promise<void> {
    if (this.usStatesGeoJson) {
      return;
    }

    try {
      const response = await fetch('https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json');
      if (!response.ok) {
        throw new Error(`Failed loading state boundaries: HTTP ${response.status}`);
      }

      this.usStatesGeoJson = (await response.json()) as UsStatesFeatureCollection;
    } catch (err) {
      this.errorMessage = `Unable to load US state labels: ${String(err)}`;
    }
  }

  private findStateForPoint(lon: number, lat: number): string | null {
    if (!this.usStatesGeoJson) {
      return null;
    }

    for (const feature of this.usStatesGeoJson.features) {
      if (this.pointInFeature(lon, lat, feature)) {
        return feature.properties?.name ?? null;
      }
    }

    return null;
  }

  private pointInFeature(lon: number, lat: number, feature: UsStateFeature): boolean {
    if (feature.geometry.type === 'Polygon') {
      return this.pointInPolygon(lon, lat, feature.geometry.coordinates as number[][][]);
    }

    const polygons = feature.geometry.coordinates as number[][][][];
    for (const polygon of polygons) {
      if (this.pointInPolygon(lon, lat, polygon)) {
        return true;
      }
    }
    return false;
  }

  private pointInPolygon(lon: number, lat: number, polygon: number[][][]): boolean {
    if (polygon.length === 0) {
      return false;
    }

    if (!this.pointInRing(lon, lat, polygon[0])) {
      return false;
    }

    for (let i = 1; i < polygon.length; i += 1) {
      if (this.pointInRing(lon, lat, polygon[i])) {
        return false;
      }
    }

    return true;
  }

  private pointInRing(lon: number, lat: number, ring: number[][]): boolean {
    let inside = false;
    for (let i = 0, j = ring.length - 1; i < ring.length; j = i, i += 1) {
      const xi = ring[i][0];
      const yi = ring[i][1];
      const xj = ring[j][0];
      const yj = ring[j][1];

      const intersects =
        (yi > lat) !== (yj > lat) &&
        lon < ((xj - xi) * (lat - yi)) / ((yj - yi) || Number.EPSILON) + xi;

      if (intersects) {
        inside = !inside;
      }
    }
    return inside;
  }

  private featureCenter(feature: UsStateFeature): [number, number] {
    if (feature.geometry.type === 'Polygon') {
      return this.ringCenter(feature.geometry.coordinates[0] as number[][]);
    }

    const polygons = feature.geometry.coordinates as number[][][][];
    let bestRing: number[][] = polygons[0][0] ?? [];
    let bestArea = this.ringArea(bestRing);

    for (const polygon of polygons) {
      const ring = polygon[0] ?? [];
      const area = this.ringArea(ring);
      if (area > bestArea) {
        bestArea = area;
        bestRing = ring;
      }
    }

    return this.ringCenter(bestRing);
  }

  private ringCenter(ring: number[][]): [number, number] {
    if (ring.length === 0) {
      return [39.5, -98.35];
    }

    let sumLon = 0;
    let sumLat = 0;
    for (const coord of ring) {
      sumLon += coord[0];
      sumLat += coord[1];
    }

    return [sumLat / ring.length, sumLon / ring.length];
  }

  private ringArea(ring: number[][]): number {
    if (ring.length < 3) {
      return 0;
    }

    let area = 0;
    for (let i = 0, j = ring.length - 1; i < ring.length; j = i, i += 1) {
      area += ring[j][0] * ring[i][1] - ring[i][0] * ring[j][1];
    }

    return Math.abs(area / 2);
  }

  private pickHeatColor(intensity: number): string {
    if (intensity > 0.75) {
      return '#ff4f70';
    }
    if (intensity > 0.45) {
      return '#ff8c42';
    }
    return '#ffd35c';
  }

  private stateLabelStyle(cases: number, maxCases: number): string {
    if (cases <= 0) {
      return 'background:#e2e6eb;color:#58616a;border-color:#b6bec8;';
    }

    const intensity = Math.min(1, cases / maxCases);
    const color = this.pickHeatColor(intensity);
    const textColor = intensity > 0.45 ? '#fff7f7' : '#2c1f14';
    return `background:${color};color:${textColor};border-color:${color};`;
  }

  private dateKeyFromNow(offsetDays: number): string {
    const date = new Date();
    date.setDate(date.getDate() + offsetDays);
    return date.toISOString().slice(0, 10);
  }

  private h3ToLatLon(h3Id: string): [number, number] {
    const anyH3 = H3 as unknown as {
      cellToLatLng?: (id: string) => [number, number];
      h3ToGeo?: (id: string) => [number, number];
    };

    if (typeof anyH3.cellToLatLng === 'function') {
      return anyH3.cellToLatLng(h3Id);
    }

    if (typeof anyH3.h3ToGeo === 'function') {
      return anyH3.h3ToGeo(h3Id);
    }

    throw new Error('No supported H3 decode method found in h3-js');
  }
}
