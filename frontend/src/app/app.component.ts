import { AfterViewInit, Component } from '@angular/core';
import * as L from 'leaflet';
import * as H3 from 'h3-js';
import {
  ParquetDataService,
  ParquetMetadataSnapshot,
  ParquetQueryFilters,
} from './services/parquet-data.service';
import { ConditionOption } from './models/condition-option.model';
import { DatabaseOverviewStats } from './models/database-overview-stats.model';
import { DemographicOption } from './models/demographic-option.model';
import { FilteredStats } from './models/filtered-stats.model';
import { HotspotPoint } from './models/hotspot-point.model';
import { MapLayerContext } from './models/map-layer-context.model';
import { HotspotAnimationService } from './services/hotspot-animation.service';
import { MapFacadeService } from './services/map-facade.service';
import * as MapHelpers from './utils/map-helpers';
import { dateKeyFromDate, normalizeDateKey } from './utils/date-helpers';

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
  private readonly latestDataDateByCondition = new Map<string, string>();

  private mapContext?: MapLayerContext;
  private h3Reference: Record<string, { latitude: number; longitude: number }> = {};

  constructor(
    private readonly parquetDataService: ParquetDataService,
    private readonly hotspotAnimationService: HotspotAnimationService,
    private readonly mapFacadeService: MapFacadeService
  ) {}

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
  lastUpdatedLabel = this.getUiLastUpdatedLabel();
  loading = false;
  errorMessage = '';

  private hotspots: HotspotPoint[] = [];

  private readonly MAX_PULSE_DURATION_MS = 18000;
  private readonly centerDotMinZoom = 6;

  get isAnimating(): boolean {
    return this.hotspotAnimationService.isAnimating;
  }

  get currentAnimationDate(): string {
    return this.hotspotAnimationService.currentAnimationDate;
  }

  private get ANIMATION_SPEED_MS(): number {
    if (this.selectedWindowDays >= 30) {
      // Keep 30+ day pulse runs under ~20s including render overhead.
      return Math.max(80, Math.floor(this.MAX_PULSE_DURATION_MS / this.selectedWindowDays));
    }
    if (this.selectedWindowDays > 20) return 500;  // 0.5s/day for >20 days
    return 1000;                                   // 1s/day for ≤20 days
  }

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
    this.lastUpdatedLabel = this.getUiLastUpdatedLabel();
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

    const animationCondition = this.selectedCondition;
    const animationEndDate = await this.getWindowAnchorDate(animationCondition);

    await this.hotspotAnimationService.start({
      conditionCode: animationCondition,
      selectedWindowDays: this.selectedWindowDays,
      endDate: animationEndDate,
      chunkSize: 10,
      animationSpeedMs: this.ANIMATION_SPEED_MS,
      resolveConditionDisplay: (conditionCode: string) =>
        this.conditionOptions.find((c) => c.code === conditionCode)?.display ?? conditionCode,
      getLatLonForH3: (h3: string) => {
        const h3Data = this.h3Reference[h3];
        return h3Data ? [h3Data.latitude, h3Data.longitude] : MapHelpers.h3ToLatLon(H3, h3);
      },
      buildFilters: (conditionCode: string, startDate: string, endDate: string) =>
        this.buildParquetFilters(conditionCode, startDate, endDate),
      onFrame: (frame) => {
        this.hotspots = frame.hotspots;
        const totalCases = frame.hotspots.reduce((sum, hotspot) => sum + hotspot.cases, 0);
        this.filteredStats = {
          totalCases,
          cellCount: frame.hotspots.length,
          activeDays: frame.activeDays,
        };
        this.renderHotspots();
      },
      onComplete: () => {
        this.stopAnimation();
      },
      onError: (err: unknown) => {
        this.errorMessage = `Animation failed: ${String(err)}`;
        console.error('Animation error:', err);
        this.stopAnimation();
      },
    });
  }

  stopAnimation(): void {
    if (this.isAnimating) {
      this.hotspotAnimationService.stop();
    }

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

    this.lastUpdatedLabel = this.getUiLastUpdatedLabel();

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
      const startDate = dateKeyFromDate(endDate, -(this.selectedWindowDays - 1));

      const response = await this.parquetDataService.queryAggregatedHotspots(
        this.buildParquetFilters(this.selectedCondition, startDate, endDate)
      );

      const selectedConditionDisplay = this.conditionOptions.find((c) => c.code === this.selectedCondition)?.display ?? this.selectedCondition;
      const points: HotspotPoint[] = [];

      for (const row of response.rows) {
        const h3Data = this.h3Reference[row.h3];
        const [lat, lon] = h3Data ? [h3Data.latitude, h3Data.longitude] : MapHelpers.h3ToLatLon(H3, row.h3);
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
      this.lastUpdatedLabel = this.getUiLastUpdatedLabel();
      this.renderHotspots();
    } catch (err) {
      this.errorMessage = `Failed to query parquet daily cells: ${String(err)}`;
      this.hotspots = [];
      this.filteredStats = { totalCases: 0, cellCount: 0, activeDays: 0 };
      this.renderHotspots();
      console.error('Parquet filter error:', err);
    } finally {
      this.loading = false;
    }
  }

  private initMap(): void {
    this.mapFacadeService.destroyMap(this.mapContext);
    this.mapContext = this.mapFacadeService.initializeMap('health-map', this.globalBounds, () => {
      this.renderHotspots();
    });
  }

  private renderHotspots(): void {
    if (!this.mapContext) {
      return;
    }

    this.mapFacadeService.renderHotspots(
      this.mapContext,
      this.hotspots,
      this.isAnimating,
      this.centerDotMinZoom
    );

    // State overlays are intentionally disabled.
  }

  private async getWindowAnchorDate(conditionCode: string): Promise<string> {
    if (this.latestDataDateByCondition.has(conditionCode)) {
      return this.latestDataDateByCondition.get(conditionCode)!;
    }

    try {
      const latestDataDate = await this.parquetDataService.getLatestDataDate(conditionCode);
      const normalizedLatestDataDate = normalizeDateKey(latestDataDate ?? '');
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

  private getUiLastUpdatedLabel(): string {
    const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000);
    return fiveMinutesAgo.toLocaleString(undefined, {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
    });
  }
}
