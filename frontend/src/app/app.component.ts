import { AfterViewInit, Component } from '@angular/core';
import * as L from 'leaflet';
import * as H3 from 'h3-js';
import {
  ParquetDataService,
  ParquetMetadataSnapshot,
  ParquetQueryFilters,
} from './services/parquet-data.service';

interface HotspotPoint {
  lat: number;
  lon: number;
  h3: string;
  conditionCode: string;
  conditionDisplay: string;
  cases: number;
  originalCases?: number; // Original case count before merging (for consistent color)
  latestDate: string;
  daysWithCases: number;
  mergedCount?: number;
}

interface ConditionOption {
  code: string;
  display: string;
  patientCount: number;
}

interface DemographicOption {
  value: string;
  label: string;
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
  private readonly globalBounds: L.LatLngBoundsExpression = [
    [-85, -180],
    [85, 180],
  ];
  private readonly showStatePolygonsByDefault = false;
  private readonly showStateLabelsByDefault = false;
  private readonly latestDataDateByCondition = new Map<string, string>();

  private map?: L.Map;
  private baseTileLayer?: L.TileLayer;
  private labelTileLayer?: L.TileLayer;
  private hotspotLayer?: L.LayerGroup;
  private cityRiskLayer?: L.LayerGroup;
  private stateBoundaryLayer?: L.LayerGroup;
  private stateLabelLayer?: L.LayerGroup;
  private usStatesGeoJson?: UsStatesFeatureCollection;
  private h3Reference: Record<string, { latitude: number; longitude: number }> = {};

  constructor(private readonly parquetDataService: ParquetDataService) {}

  conditionOptions: ConditionOption[] = [];
  readonly timeWindowOptions = [
    { label: '1D', days: 1 },
    { label: '7D', days: 7 },
    { label: '30D', days: 30 },
    { label: '90D', days: 90 },
    { label: '180D', days: 180 },
  ];

  // Demographic filter options
  ageGroupOptions: DemographicOption[] = [];
  genderOptions: DemographicOption[] = [];
  selectedAgeGroups: Set<string> = new Set();
  selectedGenders: Set<string> = new Set();

  selectedCondition = '';
  selectedWindowDays = 30;
  customWindowDays = 30;
  lastUpdatedLabel = 'Not synced yet';
  loading = false;
  errorMessage = '';

  private hotspots: HotspotPoint[] = [];

  // Animation state
  isAnimating = false;
  currentAnimationDate = '';
  private animationIntervalId?: ReturnType<typeof setTimeout>;
  private allDates: string[] = [];
  private currentDateIndex = 0;
  private readonly DATA_START_DATE = '2025-10-27'; // First day of data
  private readonly DATA_END_DATE = '2026-04-25'; // Last day of data
  private readonly MAX_PULSE_DURATION_MS = 18000;
  private readonly centerDotMinZoom = 6;

  private get ANIMATION_SPEED_MS(): number {
    if (this.selectedWindowDays >= 30) {
      // Keep 30+ day pulse runs under ~20s including render overhead.
      return Math.max(80, Math.floor(this.MAX_PULSE_DURATION_MS / this.selectedWindowDays));
    }
    if (this.selectedWindowDays > 20) return 500;  // 0.5s/day for >20 days
    return 1000;                                   // 1s/day for ≤20 days
  }

  // Data cache for animation
  private preloadedDataCache: Map<string, HotspotPoint[]> = new Map();
  private cumulativeDataCache: Map<string, HotspotPoint[]> = new Map();
  private animationActiveDaysByDate: Map<string, number> = new Map();
  private animationFinalRawPoints: HotspotPoint[] = [];
  private animationPrefetchIndex = 0;
  private readonly CHUNK_SIZE = 10;
  private stateBoundaryRenderToken = 0;
  private stateLabelRenderToken = 0;
  private isRestoringAfterAnimation = false;

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
    this.initMap();
    await this.loadOverviewAndConditions();
    // Add keyboard listener for ESC key
    document.addEventListener('keydown', (event: KeyboardEvent) => {
      if (event.key === 'Escape' && this.isAnimating) {
        this.stopAnimation();
      }
    });
  }

  async onConditionChange(): Promise<void> {
    if (this.isAnimating) {
      this.stopAnimation();
    }
    this.lastUpdatedLabel = 'Not synced yet';
    await this.refreshForCurrentFilters();
  }

  async setTimeWindow(days: number): Promise<void> {
    if (this.isAnimating) {
      this.stopAnimation();
    }
    this.selectedWindowDays = days;
    this.customWindowDays = days;
    await this.refreshForCurrentFilters();
  }

  async normalizeCustomWindow(): Promise<void> {
    if (this.isAnimating) {
      this.stopAnimation();
    }
    const bounded = Math.max(1, Math.min(180, Number(this.customWindowDays) || 30));
    this.selectedWindowDays = bounded;
    this.customWindowDays = bounded;
    await this.refreshForCurrentFilters();
  }

  toggleAgeGroup(ageGroup: string): void {
    if (this.isAnimating) {
      this.stopAnimation();
    }
    if (this.selectedAgeGroups.has(ageGroup)) {
      this.selectedAgeGroups.delete(ageGroup);
    } else {
      this.selectedAgeGroups.add(ageGroup);
    }
    console.log('Selected age groups:', Array.from(this.selectedAgeGroups));
    this.refreshForCurrentFilters().catch((err) => {
      this.errorMessage = `Failed to refresh filters: ${String(err)}`;
    });
  }

  toggleGender(gender: string): void {
    if (this.isAnimating) {
      this.stopAnimation();
    }
    if (this.selectedGenders.has(gender)) {
      this.selectedGenders.delete(gender);
    } else {
      this.selectedGenders.add(gender);
    }
    console.log('Selected genders:', Array.from(this.selectedGenders));
    this.refreshForCurrentFilters().catch((err) => {
      this.errorMessage = `Failed to refresh filters: ${String(err)}`;
    });
  }

  isAgeGroupSelected(ageGroup: string): boolean {
    return this.selectedAgeGroups.has(ageGroup);
  }

  isGenderSelected(gender: string): boolean {
    return this.selectedGenders.has(gender);
  }

  async startAnimation(): Promise<void> {
    if (this.isAnimating || !this.selectedCondition) {
      return;
    }

    this.isAnimating = true;
    this.currentDateIndex = 0;
    this.animationPrefetchIndex = 0;

    const animationCondition = this.selectedCondition;
    const animationEndDate = await this.getWindowAnchorDate(animationCondition);
    const animationStartDate = this.dateKeyFromDate(animationEndDate, -(this.selectedWindowDays - 1));

    // Generate all dates from start to end
    this.allDates = this.generateDateRange(animationStartDate, animationEndDate);

    if (this.allDates.length === 0) {
      this.isAnimating = false;
      return;
    }

    // Clear previous cache
    this.preloadedDataCache.clear();
    this.cumulativeDataCache.clear();
    this.animationActiveDaysByDate.clear();
    this.animationFinalRawPoints = [];

    // Fetch final aggregated data for sizing/coloring scale before starting
    try {
      const response = await this.parquetDataService.queryAggregatedHotspots(
        this.buildParquetFilters(animationCondition, animationStartDate, animationEndDate)
      );
      const selectedConditionDisplay = this.conditionOptions.find((c) => c.code === animationCondition)?.display ?? animationCondition;

      for (const row of response.rows) {
          const h3Data = this.h3Reference[row.h3];
          const [lat, lon] = h3Data ? [h3Data.latitude, h3Data.longitude] : this.h3ToLatLon(row.h3);

          const rawCaseCount = row.caseCount;
          const caseCount = Number(rawCaseCount);
          if (isNaN(caseCount) || caseCount <= 0) continue;

          this.animationFinalRawPoints.push({
            lat, lon, h3: row.h3,
            conditionCode: animationCondition,
            conditionDisplay: selectedConditionDisplay,
            cases: caseCount,
            originalCases: caseCount,
            latestDate: row.latestDate || animationEndDate,
            daysWithCases: Math.max(1, Number(row.daysWithCases) || 1),
          });
      }
    } catch (err) {
      console.error('Error fetching final aggregated data for animation', err);
      this.stopAnimation();
      return;
    }

    // Start background prefetching of chunks
    void this.prefetchNextChunk();

    // Start animation loop
    this.animateNextDay();
  }

  stopAnimation(): void {
    if (this.animationIntervalId) {
      clearTimeout(this.animationIntervalId);
      this.animationIntervalId = undefined;
    }
    this.isAnimating = false;
    this.currentAnimationDate = '';
    this.preloadedDataCache.clear();
    this.cumulativeDataCache.clear();
    this.animationActiveDaysByDate.clear();
    this.animationFinalRawPoints = [];
    this.hotspots = [];
    this.filteredStats = { totalCases: 0, cellCount: 0, activeDays: 0 };
    this.renderHotspots();
    // Restore normal view after animation ends
    if (!this.isRestoringAfterAnimation) {
      this.isRestoringAfterAnimation = true;
      void this.refreshForCurrentFilters().finally(() => {
        this.isRestoringAfterAnimation = false;
      });
    }
  }

  private async prefetchNextChunk(): Promise<void> {
    if (!this.isAnimating || this.animationPrefetchIndex >= this.allDates.length) return;

    const startIndex = this.animationPrefetchIndex;
    const endIndex = Math.min(startIndex + this.CHUNK_SIZE - 1, this.allDates.length - 1);
    const chunkStartDate = this.allDates[startIndex];
    const chunkEndDate = this.allDates[endIndex];

    try {
      const response = await this.parquetDataService.queryDailySeries(
        this.buildParquetFilters(this.selectedCondition, chunkStartDate, chunkEndDate)
      );

      const dataByDate = new Map<string, Array<{ h3: string; caseCount: number }>>();

      for (const row of response) {
        const dateKey = row.dateKey;
        if (!dateKey) continue;

        const rawCaseCount = row.caseCount;
        const caseCount = Number(rawCaseCount);
        if (isNaN(caseCount) || caseCount <= 0) continue;

        if (!dataByDate.has(dateKey)) dataByDate.set(dateKey, []);
        dataByDate.get(dateKey)!.push({
          h3: row.h3,
          caseCount,
        });
      }

      const selectedConditionDisplay =
        this.conditionOptions.find((c) => c.code === this.selectedCondition)?.display ??
        this.selectedCondition;

      // Process chunk dates sequentially to build cumulative cache correctly.
      for (let i = startIndex; i <= endIndex; i++) {
        const date = this.allDates[i];
        const records = dataByDate.get(date) ?? [];

        const byH3 = new Map<string, number>();
        for (const record of records) {
          byH3.set(record.h3, (byH3.get(record.h3) ?? 0) + record.caseCount);
        }

        const hotspots: HotspotPoint[] = Array.from(byH3.entries()).map(([h3, caseCount]) => {
          const h3Data = this.h3Reference[h3];
          const [lat, lon] = h3Data ? [h3Data.latitude, h3Data.longitude] : this.h3ToLatLon(h3);
          return {
            lat,
            lon,
            h3,
            conditionCode: this.selectedCondition,
            conditionDisplay: selectedConditionDisplay,
            cases: caseCount,
            originalCases: caseCount,
            latestDate: date,
            daysWithCases: 1,
          };
        });

        this.preloadedDataCache.set(date, hotspots);

        // Build cumulative for this date based on previous date's cumulative.
        const prevDate = i > 0 ? this.allDates[i - 1] : null;
        const prevCumulative = prevDate ? (this.cumulativeDataCache.get(prevDate) ?? []) : [];

        const cumulativeByH3 = new Map<string, HotspotPoint>();
        for (const p of prevCumulative) cumulativeByH3.set(p.h3, { ...p });

        for (const p of hotspots) {
          const existing = cumulativeByH3.get(p.h3);
          if (!existing) {
            cumulativeByH3.set(p.h3, { ...p });
          } else {
            existing.cases += p.cases;
            existing.originalCases = (existing.originalCases ?? 0) + (p.originalCases ?? p.cases);
            existing.daysWithCases += 1;
            if (p.latestDate > existing.latestDate) {
              existing.latestDate = p.latestDate;
            }
          }
        }

        this.cumulativeDataCache.set(date, Array.from(cumulativeByH3.values()));

        const prevActiveDays = prevDate ? (this.animationActiveDaysByDate.get(prevDate) ?? 0) : 0;
        this.animationActiveDaysByDate.set(date, prevActiveDays + (hotspots.length > 0 ? 1 : 0));
      }

      this.animationPrefetchIndex = endIndex + 1;
    } catch (err) {
      console.error('Error prefetching animation chunk:', err);
      this.stopAnimation();
      return;
    }

    // Automatically trigger next chunk fetch
    void this.prefetchNextChunk();
  }

  private animateNextDay(): void {
    if (!this.isAnimating) {
      return;
    }

    if (this.currentDateIndex >= this.allDates.length) {
      this.stopAnimation();
      return;
    }

    const currentDate = this.allDates[this.currentDateIndex];
    
    // Check if this date is loaded yet (waiting for prefetch)
    if (!this.cumulativeDataCache.has(currentDate)) {
      this.animationIntervalId = setTimeout(() => {
        this.animateNextDay();
      }, 100);
      return;
    }

    this.currentAnimationDate = currentDate;

    // Get cached data for this date
    const cachedData = this.cumulativeDataCache.get(currentDate) ?? [];
    this.hotspots = cachedData;

    // Update stats for animation display
    let totalCases = 0;
    for (const hotspot of this.hotspots) {
      totalCases += hotspot.cases;
    }
    this.filteredStats = {
      totalCases,
      cellCount: this.hotspots.length,
      activeDays: this.animationActiveDaysByDate.get(currentDate) ?? 0,
    };

    this.renderHotspots();

    this.currentDateIndex++;

    if (this.currentDateIndex >= this.allDates.length) {
      this.stopAnimation();
      return;
    }

    // Schedule next day with smooth timing (avoid interval stacking)
    this.animationIntervalId = setTimeout(() => {
      this.animateNextDay();
    }, this.ANIMATION_SPEED_MS);
  }

  private generateDateRange(startDateStr: string, endDateStr: string): string[] {
    const dates: string[] = [];
    const currentDate = new Date(startDateStr);
    const endDate = new Date(endDateStr);

    while (currentDate <= endDate) {
      const dateStr = currentDate.toISOString().slice(0, 10);
      dates.push(dateStr);
      currentDate.setDate(currentDate.getDate() + 1);
    }

    return dates;
  }

  private async loadOverviewAndConditions(): Promise<void> {
    this.loading = true;
    this.errorMessage = '';
    let shouldRefresh = false;

    try {
      const metadata = await this.parquetDataService.loadMetadata();
      this.applyMetadata(metadata);
      shouldRefresh = true;
    } catch (err) {
      this.errorMessage = `Failed to load parquet metadata: ${String(err)}`;
      console.error('Parquet metadata load error:', err);
    } finally {
      this.loading = false;
    }

    if (shouldRefresh) {
      await this.refreshForCurrentFilters();
    }
  }

  private applyMetadata(metadata: ParquetMetadataSnapshot): void {
    this.totalStats = {
      totalConditions: metadata.totalConditions,
      totalPatients: metadata.totalPatients,
      totalCases: metadata.totalCases,
      totalCells: metadata.totalCells,
    };

    const parsedLastUpdated = new Date(metadata.lastUpdatedIso);
    this.lastUpdatedLabel = Number.isNaN(parsedLastUpdated.getTime())
      ? metadata.lastUpdatedIso
      : parsedLastUpdated.toLocaleString();

    const ageGroups = metadata.ageGroups.length > 0
      ? metadata.ageGroups
      : ['0-17', '18-34', '35-54', '55+', 'Unknown'];

    const genders = metadata.genders.length > 0
      ? metadata.genders
      : ['male', 'female', 'other', 'unknown'];

    this.ageGroupOptions = ageGroups.map((ageGroup) => ({
      value: ageGroup,
      label: ageGroup === 'Unknown' ? 'Unknown' : ageGroup,
    }));
    this.selectedAgeGroups = new Set(ageGroups);

    this.genderOptions = genders.map((gender) => ({
      value: gender,
      label: gender.charAt(0).toUpperCase() + gender.slice(1),
    }));
    this.selectedGenders = new Set(genders);

    this.conditionOptions = metadata.conditions.map((condition) => ({
      code: condition.code,
      display: condition.display,
      patientCount: condition.patientCount,
    }));

    this.h3Reference = metadata.h3Reference;

    if (!this.selectedCondition && this.conditionOptions.length > 0) {
      this.selectedCondition = this.conditionOptions[0].code;
    }
  }

  private buildParquetFilters(
    conditionCode: string,
    startDate: string,
    endDate: string
  ): ParquetQueryFilters {
    return {
      conditionCode,
      startDate,
      endDate,
      ageGroups: Array.from(this.selectedAgeGroups),
      genders: Array.from(this.selectedGenders),
    };
  }

  private async refreshForCurrentFilters(): Promise<void> {
    if (!this.selectedCondition) {
      this.hotspots = [];
      this.filteredStats = { totalCases: 0, cellCount: 0, activeDays: 0 };
      this.renderHotspots();
      return;
    }

    await this.refreshParquetFilters();
  }

  private async refreshParquetFilters(): Promise<void> {
    this.loading = true;
    this.errorMessage = '';

    try {
      if (this.selectedAgeGroups.size === 0 || this.selectedGenders.size === 0) {
        this.hotspots = [];
        this.filteredStats = {
          totalCases: 0,
          cellCount: 0,
          activeDays: 0,
        };
        this.renderHotspots();
        return;
      }

      const endDate = await this.getWindowAnchorDate(this.selectedCondition);
      const startDate = this.dateKeyFromDate(endDate, -(this.selectedWindowDays - 1));

      const response = await this.parquetDataService.queryAggregatedHotspots(
        this.buildParquetFilters(this.selectedCondition, startDate, endDate)
      );

      const selectedConditionDisplay = this.conditionOptions.find((c) => c.code === this.selectedCondition)?.display ?? this.selectedCondition;
      const points: HotspotPoint[] = [];

      for (const row of response.rows) {
        const h3Data = this.h3Reference[row.h3];
        const [lat, lon] = h3Data ? [h3Data.latitude, h3Data.longitude] : this.h3ToLatLon(row.h3);
        const caseCount = Number(row.caseCount);
        if (isNaN(caseCount) || caseCount <= 0) {
          continue;
        }

        points.push({
          lat,
          lon,
          h3: row.h3,
          conditionCode: this.selectedCondition,
          conditionDisplay: selectedConditionDisplay,
          cases: caseCount,
          originalCases: caseCount,
          latestDate: row.latestDate || endDate,
          daysWithCases: Math.max(1, Number(row.daysWithCases) || 1),
        });
      }

      this.hotspots = points;
      this.filteredStats = {
        totalCases: response.totalCases,
        cellCount: points.length,
        activeDays: response.activeDays,
      };
      this.lastUpdatedLabel = endDate;
      this.renderHotspots();
    } catch (err) {
      this.errorMessage = `Failed to query parquet daily cells: ${String(err)}`;
      this.hotspots = [];
      this.filteredStats = { totalCases: 0, cellCount: 0, activeDays: 0 };
      this.renderHotspots();
      console.error('Parquet filter error:', err);
      this.hotspots = [];
      this.filteredStats = { totalCases: 0, cellCount: 0, activeDays: 0 };
      this.renderHotspots();
    } finally {
      this.loading = false;
    }
  }

  private initMap(): void {
    if (this.map) {
      this.map.off();
      this.map.remove();
      this.map = undefined;
    }

    this.map = L.map('health-map', {
      zoomControl: true,
      attributionControl: true,
      maxBounds: this.globalBounds,
      maxBoundsViscosity: 1.0,
      minZoom: 2,
    }).fitBounds(this.globalBounds);

    if (this.baseTileLayer) {
      this.baseTileLayer.remove();
    }
    if (this.labelTileLayer) {
      this.labelTileLayer.remove();
      this.labelTileLayer = undefined;
    }

    this.baseTileLayer = L.tileLayer(
      'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png',
      {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
        subdomains: 'abcd',
        noWrap: true,
        bounds: [[-90, -180], [90, 180]],
        minZoom: 0,
        maxZoom: 12,
        // Performance tuning
        keepBuffer: 4,            // preload 4 tiles outside the viewport
        updateWhenIdle: false,    // update tiles while panning, not just after
        updateWhenZooming: false, // skip intermediate zoom frames
        crossOrigin: true,        // enables browser cache reuse across sessions
      } as L.TileLayerOptions
    ).addTo(this.map);

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

    // Re-render hotspots once zoom animation completes (zoomend fires once, not per frame)
    this.map.on('zoomend', () => {
      this.renderHotspots();
    });
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
    
    // For animation to scale correctly, we need the max values from the final day at the current zoom
    let maxColorCases = 1;
    let maxMergedCases = 1;
    
    if (this.isAnimating && this.animationFinalRawPoints.length > 0) {
      const finalMergedPoints = this.mergeCoincidentHotspots(this.animationFinalRawPoints);
      maxColorCases = finalMergedPoints.reduce((max, p) => Math.max(max, p.originalCases ?? p.cases ?? 0), 1);
      maxMergedCases = finalMergedPoints.reduce((max, p) => Math.max(max, p.cases ?? 0), 1);
    } else {
      maxColorCases = points.reduce((max, p) => Math.max(max, p.originalCases ?? p.cases ?? 0), 1);
      maxMergedCases = points.reduce((max, p) => Math.max(max, p.cases ?? 0), 1);
    }
    
    if (isNaN(maxColorCases) || maxColorCases <= 0) maxColorCases = 1;
    if (isNaN(maxMergedCases) || maxMergedCases <= 0) maxMergedCases = 1;

    for (const point of points) {
      // Color based purely on absolute case count in the H3 cell
      const colorBase = point.originalCases ?? point.cases;
      const fillColor = this.getH3Color(colorBase);
      
      // Size based on absolute case count (capped at 50,000 for max size)
      const sizeIntensity = Math.min(1, point.cases / 50000);
      // Opacity also scales with case count so low-case cells are visually lighter
      const opacityIntensity = Math.min(1, point.cases / 50000);
      
      // Scale radius based on intensity for heatmap effect
      // Zoom-aware sizing: reduces circle size as user zooms in
      const currentZoom = this.map?.getZoom() ?? 2;
      const baseRadiusMeters = this.getMainCircleRadiusMeters(point.cases, currentZoom);
    

      const haloMultiplier = 1.4;
      const haloOpacity = Math.max(0.2, opacityIntensity * 0.15);
      const strokeOpacity = 1;
      const strokeWeight = 1;
      const fillOpacity = this.getMainFillOpacity(fillColor, sizeIntensity);

      // Outer halo - semi-transparent larger circle
      const haloCircle = L.circle([point.lat, point.lon], {
        pane: 'hotspotPane',
        radius: baseRadiusMeters * haloMultiplier,
        className: 'hotspot-halo-circle',
        color: fillColor,
        weight: 0,
        fillColor,
        fillOpacity: haloOpacity,
      }).addTo(this.hotspotLayer);

      // Main hotspot circle
      const mainCircle = L.circle([point.lat, point.lon], {
        pane: 'hotspotPane',
        radius: baseRadiusMeters,
        className: 'hotspot-main-circle',
        color: fillColor,
        weight: strokeWeight,
        opacity: strokeOpacity,
        dashArray: '5, 5',
        fillColor,
        fillOpacity,
      })
        .bindPopup(
          `<strong>${point.conditionDisplay}</strong><br/><strong>Cases:</strong> ${point.cases.toLocaleString()}<br/><strong>Days:</strong> ${point.daysWithCases}${point.mergedCount && point.mergedCount > 1 ? `<br/><strong>Merged Hotspots:</strong> ${point.mergedCount}` : ''}<br/><strong>Lat/Lon:</strong> ${point.lat.toFixed(6)}, ${point.lon.toFixed(6)}<br/><strong>H3:</strong> ${point.h3}`
        )
        .addTo(this.hotspotLayer);

      if (currentZoom >= this.centerDotMinZoom) {
        const zoomBoost = Math.max(0, currentZoom - this.centerDotMinZoom);
        const centerDotRadiusPx = Math.min(5.5, 2.2 + sizeIntensity * 1.6 + zoomBoost * 0.25);

        L.circleMarker([point.lat, point.lon], {
          pane: 'hotspotPane',
          className: 'hotspot-center-dot',
          radius: centerDotRadiusPx,
          color: 'transparent',
          weight: 1,
          opacity: 0.98,
          fillColor,
          fillOpacity: 0.97,
          interactive: false,
        }).addTo(this.hotspotLayer);
      }

      // Add blinking effect during animation
      if (this.isAnimating) {
        // Add animation class to both circles
        const haloEl = haloCircle.getElement();
        const mainEl = mainCircle.getElement();
        if (haloEl) {
          haloEl.classList.add('hotspot-pulse');
        }
        if (mainEl) {
          mainEl.classList.add('hotspot-pulse');
        }
      }
    }

    if (this.showStatePolygonsByDefault) {
      void this.renderStateBoundaries();
    }

    // Don't auto-fit map to hotspots - keep user's current view
    // if (points.length > 0) {
    //   const bounds = L.latLngBounds(points.map((p) => [p.lat, p.lon] as [number, number]));
    //   this.map.fitBounds(bounds.pad(0.35));
    //   this.map.panInsideBounds(this.globalBounds, { animate: false });
    // }

    if (this.showStateLabelsByDefault) {
      void this.renderStateLabels(points);
    }
  }

  private getMainCircleRadiusMeters(cases: number, zoom: number): number {
    const sizeIntensity = Math.min(1, cases / 50000);
    const zoomDelta = Math.max(0, zoom - 2);
    const zoomScale = Math.exp(-0.62 * zoomDelta);
    const radiusByZoom = (120000 + sizeIntensity * 180000) * zoomScale;
    const minRadiusMeters = 900 + 4100 / (1 + Math.exp(0.9 * (zoom - 6)));
    return Math.max(minRadiusMeters, radiusByZoom) * 1.3;
  }

  private mergeCoincidentHotspots(points: HotspotPoint[]): HotspotPoint[] {
    // First, deduplicate by exact lat/lon to prevent same location appearing twice
    const deduplicated = new Map<string, HotspotPoint>();
    for (const point of points) {
      const key = `${point.lat.toFixed(8)}|${point.lon.toFixed(8)}`;
      if (!deduplicated.has(key)) {
        deduplicated.set(key, { ...point, originalCases: point.originalCases ?? point.cases });
      } else {
        // If duplicate found, combine cases
        const existing = deduplicated.get(key)!;
        existing.cases += point.cases;
        existing.originalCases = Math.max(existing.originalCases ?? 0, point.originalCases ?? point.cases);
        existing.mergedCount = (existing.mergedCount ?? 1) + 1;
      }
    }

    const uniquePoints = Array.from(deduplicated.values());
    const currentZoom = this.map?.getZoom() ?? 2;
    
    // Keep a zoom-level floor for merge distance, then merge whenever circles still overlap.
    const baseMergeThresholdKm =
      currentZoom <= 2 ? 900 :
      currentZoom <= 3 ? 650 :
      currentZoom <= 5 ? 240 :
      currentZoom <= 7 ? 50 :
      currentZoom <= 9 ? 15 :
      5;

    const merged: HotspotPoint[] = [];
    const processed = new Set<number>();

    for (let i = 0; i < uniquePoints.length; i++) {
      if (processed.has(i)) continue;

      const basePoint = { 
        ...uniquePoints[i], 
        mergedCount: 1,
        originalCases: uniquePoints[i].originalCases ?? uniquePoints[i].cases
      };
      processed.add(i);

      // Find nearby points to merge
      for (let j = i + 1; j < uniquePoints.length; j++) {
        if (processed.has(j)) continue;
        
        const distance = this.haversineDistance(
          basePoint.lat,
          basePoint.lon,
          uniquePoints[j].lat,
          uniquePoints[j].lon
        );

        const baseRadiusKm = this.getMainCircleRadiusMeters(basePoint.cases, currentZoom) / 1000;
        const candidateRadiusKm = this.getMainCircleRadiusMeters(uniquePoints[j].cases, currentZoom) / 1000;
        const overlapThresholdKm = baseRadiusKm + candidateRadiusKm;
        const effectiveMergeThresholdKm = Math.max(baseMergeThresholdKm, overlapThresholdKm);

        if (distance <= effectiveMergeThresholdKm) {
          basePoint.cases += uniquePoints[j].cases;
          basePoint.originalCases = Math.max(basePoint.originalCases ?? 0, uniquePoints[j].originalCases ?? uniquePoints[j].cases);
          basePoint.daysWithCases = Math.max(basePoint.daysWithCases, uniquePoints[j].daysWithCases);
          basePoint.mergedCount = (basePoint.mergedCount ?? 1) + 1;
          if (uniquePoints[j].latestDate > basePoint.latestDate) {
            basePoint.latestDate = uniquePoints[j].latestDate;
          }
          processed.add(j);
        }
      }

      merged.push(basePoint);
    }

    return merged;
  }

  private haversineDistance(lat1: number, lon1: number, lat2: number, lon2: number): number {
    const R = 6371; // Earth's radius in km
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLon = (lon2 - lon1) * Math.PI / 180;
    const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
              Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
              Math.sin(dLon / 2) * Math.sin(dLon / 2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    return R * c;
  }

  private async renderStateBoundaries(): Promise<void> {
    if (!this.stateBoundaryLayer) {
      return;
    }

    const renderToken = ++this.stateBoundaryRenderToken;
    this.stateBoundaryLayer.clearLayers();

    await this.ensureUsStatesGeoJson();
    if (renderToken !== this.stateBoundaryRenderToken) {
      return;
    }
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
          existing.daysWithCases = Math.max(existing.daysWithCases, point.daysWithCases);
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
          opacity: 0.2,
          fillColor: borderColor,
          fillOpacity: 0.1,
          weight: 0.2,
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

    const renderToken = ++this.stateLabelRenderToken;
    this.stateLabelLayer.clearLayers();

    await this.ensureUsStatesGeoJson();
    if (renderToken !== this.stateLabelRenderToken) {
      return;
    }
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

  private getMainFillOpacity(fillColor: string, sizeIntensity: number): number {
    const luminance = this.getHexLuminance(fillColor);
    const baseOpacity = 0.22 + sizeIntensity * 0.20;
    const lightColorBoost = 0.28 * luminance;
    const opacity = baseOpacity + lightColorBoost;
    return Math.max(0.24, Math.min(0.64, opacity));
  }

  private getHexLuminance(hexColor: string): number {
    const normalized = String(hexColor || '').trim().replace('#', '');
    const expanded =
      normalized.length === 3
        ? normalized
            .split('')
            .map((value) => `${value}${value}`)
            .join('')
        : normalized;

    if (!/^[0-9a-fA-F]{6}$/.test(expanded)) {
      return 0.5;
    }

    const red = parseInt(expanded.slice(0, 2), 16) / 255;
    const green = parseInt(expanded.slice(2, 4), 16) / 255;
    const blue = parseInt(expanded.slice(4, 6), 16) / 255;

    const toLinear = (channel: number): number =>
      channel <= 0.04045 ? channel / 12.92 : Math.pow((channel + 0.055) / 1.055, 2.4);

    const r = toLinear(red);
    const g = toLinear(green);
    const b = toLinear(blue);

    return 0.2126 * r + 0.7152 * g + 0.0722 * b;
  }

  // Absolute color scale for H3 cells — yellow up to ~4k, orange 4k-20k, red 20k-50k, dark red >50k
  private getH3Color(cases: number): string {
    if (cases < 2000)   return '#ffff66';    // Yellow           
    if (cases < 4000)  return '#ffee00';    // Bright Yellow    
    if (cases < 6000)  return '#ffcc00';    // Yellow-Amber 
    if (cases < 8000)  return '#ffaa00';    // Amber          
    if (cases < 12000)  return '#ff8800';    // Orange         
    if (cases < 16000) return '#ff6600';    // Dark Orange     
    if (cases < 25000) return '#ff3300';    // Red-Orange       
    return '#cc0000';                       // Dark Red         
  }

  // Relative color scale for State Labels
  private pickHeatColor(intensity: number): string {
    // Color gradient: Yellow (low) -> Dark Red (high)
    if (intensity < 0.2) {
      return '#ffff00';  // Yellow
    }
    if (intensity < 0.35) {
      return '#ffdd00';  // Yellow-Orange
    }
    if (intensity < 0.5) {
      return '#ff8800';  // Orange
    }
    if (intensity < 0.65) {
      return '#ff5500';  // Dark Orange
    }
    if (intensity < 0.8) {
      return '#ff2200';  // Red-Orange
    }
    return '#aa0000';    // Dark Red (highest)
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

  private dateKeyFromDate(baseDateKey: string, offsetDays: number): string {
    const normalizedBaseDateKey = this.normalizeDateKey(baseDateKey) ?? new Date().toISOString().slice(0, 10);
    const date = new Date(`${normalizedBaseDateKey}T00:00:00Z`);
    date.setUTCDate(date.getUTCDate() + offsetDays);
    return date.toISOString().slice(0, 10);
  }

  private async getWindowAnchorDate(conditionCode: string): Promise<string> {
    if (this.latestDataDateByCondition.has(conditionCode)) {
      return this.latestDataDateByCondition.get(conditionCode)!;
    }

    try {
      const latestDataDate = await this.parquetDataService.getLatestDataDate(conditionCode);
      const normalizedLatestDataDate = this.normalizeDateKey(latestDataDate ?? '');
      if (normalizedLatestDataDate) {
        this.latestDataDateByCondition.set(conditionCode, normalizedLatestDataDate);
        return normalizedLatestDataDate;
      }
    } catch (error) {
      console.warn(`Unable to resolve latest data date for ${conditionCode}.`, error);
    }

    const fallback = new Date().toISOString().slice(0, 10);
    this.latestDataDateByCondition.set(conditionCode, fallback);
    return fallback;
  }

  private normalizeDateKey(rawValue: string): string | null {
    const trimmed = String(rawValue || '').trim();
    if (!trimmed) {
      return null;
    }

    const exactIsoDateMatch = trimmed.match(/^(\d{4}-\d{2}-\d{2})$/);
    if (exactIsoDateMatch) {
      return exactIsoDateMatch[1];
    }

    const embeddedIsoDateMatch = trimmed.match(/(\d{4}-\d{2}-\d{2})/);
    if (embeddedIsoDateMatch) {
      return embeddedIsoDateMatch[1];
    }

    const parsedDate = new Date(trimmed);
    if (!Number.isNaN(parsedDate.getTime())) {
      return parsedDate.toISOString().slice(0, 10);
    }

    return null;
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
