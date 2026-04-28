export interface HotspotPoint {
  lat: number;
  lon: number;
  h3: string;
  conditionCode: string;
  conditionDisplay: string;
  cases: number;
  originalCases?: number;
  latestDate: string;
  daysWithCases: number;
  mergedCount?: number;
}