import { ParquetConditionOption } from './parquet-condition-option.model';

export interface ParquetMetadataSnapshot {
  lastUpdatedIso: string;
  totalConditions: number;
  totalCases: number;
  totalPatients: number;
  totalCells: number;
  ageGroups: string[];
  genders: string[];
  conditions: ParquetConditionOption[];
  h3Reference: Record<string, { latitude: number; longitude: number }>;
}