import { Injectable } from '@angular/core';
import { HotspotPoint } from '../models/hotspot-point.model';
import { HotspotVisualizationService } from './hotspot-visualization.service';

@Injectable({
  providedIn: 'root',
})
export class HotspotMergeService {
  private readonly mergeCenterWeightPower = 1.25;

  constructor(private readonly hotspotVisualizationService: HotspotVisualizationService) {}

  mergeCoincidentHotspots(points: HotspotPoint[], currentZoom: number): HotspotPoint[] {
    if (points.length <= 1) {
      return points.map((point) => ({
        ...point,
        mergedCount: point.mergedCount ?? 1,
        originalCases: point.originalCases ?? point.cases,
      }));
    }

    const uniquePoints = this.deduplicate(points).sort((a, b) => a.lat - b.lat);
    const processed = new Array<boolean>(uniquePoints.length).fill(false);
    const merged: HotspotPoint[] = [];

    const baseMergeThresholdKm = this.hotspotVisualizationService.getBaseMergeThresholdKm(currentZoom);
    const maxPossibleThresholdKm = this.hotspotVisualizationService.getMaxMergeThresholdKm(currentZoom);
    const maxLatDeltaDeg = maxPossibleThresholdKm / 110.574;

    for (let i = 0; i < uniquePoints.length; i += 1) {
      if (processed[i]) continue;

      const basePoint: HotspotPoint = {
        ...uniquePoints[i],
        mergedCount: 1,
        originalCases: uniquePoints[i].originalCases ?? uniquePoints[i].cases,
      };
      let mergeWeightSum = this.centerWeight(uniquePoints[i].cases);
      let weightedLatSum = uniquePoints[i].lat * mergeWeightSum;
      let weightedLonSum = uniquePoints[i].lon * mergeWeightSum;
      processed[i] = true;

      const lonKmPerDeg = Math.max(8, 111.32 * Math.cos((basePoint.lat * Math.PI) / 180));
      const maxLonDeltaDeg = Math.min(180, maxPossibleThresholdKm / lonKmPerDeg);

      for (let j = i + 1; j < uniquePoints.length; j += 1) {
        if (processed[j]) continue;

        const candidate = uniquePoints[j];
        if (candidate.lat - basePoint.lat > maxLatDeltaDeg) {
          break;
        }

        if (this.deltaLongitudeDegrees(basePoint.lon, candidate.lon) > maxLonDeltaDeg) {
          continue;
        }

        const distanceKm = this.haversineDistanceKm(basePoint.lat, basePoint.lon, candidate.lat, candidate.lon);
        const baseRadiusKm =
          this.hotspotVisualizationService.getMainCircleRadiusMeters(basePoint.cases, currentZoom) / 1000;
        const candidateRadiusKm =
          this.hotspotVisualizationService.getMainCircleRadiusMeters(candidate.cases, currentZoom) / 1000;
        const overlapThresholdKm = baseRadiusKm + candidateRadiusKm;
        const effectiveMergeThresholdKm = Math.max(baseMergeThresholdKm, overlapThresholdKm);

        if (distanceKm <= effectiveMergeThresholdKm) {
          const candidateWeight = this.centerWeight(candidate.cases);
          mergeWeightSum += candidateWeight;
          weightedLatSum += candidate.lat * candidateWeight;
          weightedLonSum += candidate.lon * candidateWeight;
          basePoint.lat = weightedLatSum / mergeWeightSum;
          basePoint.lon = weightedLonSum / mergeWeightSum;

          basePoint.cases += candidate.cases;
          basePoint.originalCases = Math.max(
            basePoint.originalCases ?? 0,
            candidate.originalCases ?? candidate.cases
          );
          basePoint.daysWithCases = Math.max(basePoint.daysWithCases, candidate.daysWithCases);
          basePoint.mergedCount = (basePoint.mergedCount ?? 1) + 1;
          if (candidate.latestDate > basePoint.latestDate) {
            basePoint.latestDate = candidate.latestDate;
          }
          processed[j] = true;
        }
      }

      merged.push(basePoint);
    }

    return merged;
  }

  private deduplicate(points: HotspotPoint[]): HotspotPoint[] {
    const deduplicated = new Map<string, HotspotPoint>();

    for (const point of points) {
      const key = `${point.lat.toFixed(8)}|${point.lon.toFixed(8)}`;
      const existing = deduplicated.get(key);

      if (!existing) {
        deduplicated.set(key, { ...point, originalCases: point.originalCases ?? point.cases });
        continue;
      }

      existing.cases += point.cases;
      existing.originalCases = Math.max(existing.originalCases ?? 0, point.originalCases ?? point.cases);
      existing.mergedCount = (existing.mergedCount ?? 1) + 1;
      existing.daysWithCases = Math.max(existing.daysWithCases, point.daysWithCases);
      if (point.latestDate > existing.latestDate) {
        existing.latestDate = point.latestDate;
      }
    }

    return Array.from(deduplicated.values());
  }

  private deltaLongitudeDegrees(lon1: number, lon2: number): number {
    const delta = Math.abs(lon1 - lon2);
    return delta > 180 ? 360 - delta : delta;
  }

  private haversineDistanceKm(lat1: number, lon1: number, lat2: number, lon2: number): number {
    const dLat = ((lat2 - lat1) * Math.PI) / 180;
    const dLon = ((lon2 - lon1) * Math.PI) / 180;
    const a =
      Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos((lat1 * Math.PI) / 180) * Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLon / 2) * Math.sin(dLon / 2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    return 6371 * c;
  }

  private centerWeight(cases: number): number {
    const normalizedCases = Math.max(1, cases);
    return Math.pow(normalizedCases, this.mergeCenterWeightPower);
  }
}
