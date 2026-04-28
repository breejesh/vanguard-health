import { Injectable } from '@angular/core';
import { ParquetDataService, ParquetQueryFilters } from './parquet-data.service';
import { HotspotPoint } from '../models/hotspot-point.model';
import { dateKeyFromDate } from '../utils/date-helpers';

export interface HotspotAnimationFrame {
  date: string;
  hotspots: HotspotPoint[];
  activeDays: number;
}

export interface HotspotAnimationConfig {
  conditionCode: string;
  selectedWindowDays: number;
  endDate: string;
  chunkSize: number;
  animationSpeedMs: number;
  resolveConditionDisplay: (conditionCode: string) => string;
  getLatLonForH3: (h3: string) => [number, number];
  buildFilters: (conditionCode: string, startDate: string, endDate: string) => ParquetQueryFilters;
  onFrame: (frame: HotspotAnimationFrame) => void;
  onComplete: () => void;
  onError: (error: unknown) => void;
}

@Injectable({
  providedIn: 'root',
})
export class HotspotAnimationService {
  private _isAnimating = false;
  private _currentAnimationDate = '';
  private animationIntervalId?: ReturnType<typeof setTimeout>;
  private allDates: string[] = [];
  private currentDateIndex = 0;
  private preloadedDataCache: Map<string, HotspotPoint[]> = new Map();
  private cumulativeDataCache: Map<string, HotspotPoint[]> = new Map();
  private animationActiveDaysByDate: Map<string, number> = new Map();
  private animationPrefetchIndex = 0;

  constructor(private readonly parquetDataService: ParquetDataService) {}

  get isAnimating(): boolean {
    return this._isAnimating;
  }

  get currentAnimationDate(): string {
    return this._currentAnimationDate;
  }

  async start(config: HotspotAnimationConfig): Promise<void> {
    if (this._isAnimating || !config.conditionCode) {
      return;
    }

    this._isAnimating = true;
    this.currentDateIndex = 0;
    this.animationPrefetchIndex = 0;

    const animationStartDate = dateKeyFromDate(config.endDate, -(config.selectedWindowDays - 1));
    this.allDates = this.generateDateRange(animationStartDate, config.endDate);

    if (this.allDates.length === 0) {
      this.stop();
      return;
    }

    this.preloadedDataCache.clear();
    this.cumulativeDataCache.clear();
    this.animationActiveDaysByDate.clear();

    void this.prefetchNextChunk(config);
    this.animateNextDay(config);
  }

  stop(): void {
    if (this.animationIntervalId) {
      clearTimeout(this.animationIntervalId);
      this.animationIntervalId = undefined;
    }

    this._isAnimating = false;
    this._currentAnimationDate = '';
    this.preloadedDataCache.clear();
    this.cumulativeDataCache.clear();
    this.animationActiveDaysByDate.clear();
    this.allDates = [];
    this.currentDateIndex = 0;
    this.animationPrefetchIndex = 0;
  }

  private async prefetchNextChunk(config: HotspotAnimationConfig): Promise<void> {
    if (!this._isAnimating || this.animationPrefetchIndex >= this.allDates.length) {
      return;
    }

    const startIndex = this.animationPrefetchIndex;
    const endIndex = Math.min(startIndex + config.chunkSize - 1, this.allDates.length - 1);
    const chunkStartDate = this.allDates[startIndex];
    const chunkEndDate = this.allDates[endIndex];

    try {
      const response = await this.parquetDataService.queryDailySeries(
        config.buildFilters(config.conditionCode, chunkStartDate, chunkEndDate)
      );

      const dataByDate = new Map<string, Array<{ h3: string; caseCount: number }>>();

      for (const row of response) {
        const dateKey = row.dateKey;
        if (!dateKey) continue;

        const caseCount = Number(row.caseCount);
        if (isNaN(caseCount) || caseCount <= 0) continue;

        if (!dataByDate.has(dateKey)) {
          dataByDate.set(dateKey, []);
        }

        dataByDate.get(dateKey)!.push({
          h3: row.h3,
          caseCount,
        });
      }

      const conditionDisplay = config.resolveConditionDisplay(config.conditionCode);

      for (let i = startIndex; i <= endIndex; i += 1) {
        const date = this.allDates[i];
        const records = dataByDate.get(date) ?? [];

        const byH3 = new Map<string, number>();
        for (const record of records) {
          byH3.set(record.h3, (byH3.get(record.h3) ?? 0) + record.caseCount);
        }

        const hotspots: HotspotPoint[] = Array.from(byH3.entries()).map(([h3, caseCount]) => {
          const [lat, lon] = config.getLatLonForH3(h3);
          return {
            lat,
            lon,
            h3,
            conditionCode: config.conditionCode,
            conditionDisplay,
            cases: caseCount,
            originalCases: caseCount,
            latestDate: date,
            daysWithCases: 1,
          };
        });

        this.preloadedDataCache.set(date, hotspots);

        const prevDate = i > 0 ? this.allDates[i - 1] : null;
        const prevCumulative = prevDate ? (this.cumulativeDataCache.get(prevDate) ?? []) : [];

        const cumulativeByH3 = new Map<string, HotspotPoint>();
        for (const point of prevCumulative) {
          cumulativeByH3.set(point.h3, { ...point });
        }

        for (const point of hotspots) {
          const existing = cumulativeByH3.get(point.h3);
          if (!existing) {
            cumulativeByH3.set(point.h3, { ...point });
          } else {
            existing.cases += point.cases;
            existing.originalCases = (existing.originalCases ?? 0) + (point.originalCases ?? point.cases);
            existing.daysWithCases += 1;
            if (point.latestDate > existing.latestDate) {
              existing.latestDate = point.latestDate;
            }
          }
        }

        this.cumulativeDataCache.set(date, Array.from(cumulativeByH3.values()));

        const prevActiveDays = prevDate ? (this.animationActiveDaysByDate.get(prevDate) ?? 0) : 0;
        this.animationActiveDaysByDate.set(date, prevActiveDays + (hotspots.length > 0 ? 1 : 0));
      }

      this.animationPrefetchIndex = endIndex + 1;
    } catch (err) {
      this.stop();
      config.onError(err);
      return;
    }

    void this.prefetchNextChunk(config);
  }

  private animateNextDay(config: HotspotAnimationConfig): void {
    if (!this._isAnimating) {
      return;
    }

    if (this.currentDateIndex >= this.allDates.length) {
      this.stop();
      config.onComplete();
      return;
    }

    const currentDate = this.allDates[this.currentDateIndex];
    if (!this.cumulativeDataCache.has(currentDate)) {
      this.animationIntervalId = setTimeout(() => {
        this.animateNextDay(config);
      }, 100);
      return;
    }

    this._currentAnimationDate = currentDate;
    const cachedData = this.cumulativeDataCache.get(currentDate) ?? [];

    config.onFrame({
      date: currentDate,
      hotspots: cachedData,
      activeDays: this.animationActiveDaysByDate.get(currentDate) ?? 0,
    });

    this.currentDateIndex += 1;

    if (this.currentDateIndex >= this.allDates.length) {
      this.stop();
      config.onComplete();
      return;
    }

    this.animationIntervalId = setTimeout(() => {
      this.animateNextDay(config);
    }, config.animationSpeedMs);
  }

  private generateDateRange(startDateStr: string, endDateStr: string): string[] {
    const dates: string[] = [];
    const currentDate = new Date(startDateStr);
    const endDate = new Date(endDateStr);

    while (currentDate <= endDate) {
      dates.push(currentDate.toISOString().slice(0, 10));
      currentDate.setDate(currentDate.getDate() + 1);
    }

    return dates;
  }
}
