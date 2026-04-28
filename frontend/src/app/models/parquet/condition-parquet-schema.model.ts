export interface ConditionParquetSchema {
  alias: string;
  dateColumn: string;
  h3Column: string;
  caseColumn: string;
  ageColumn?: string;
  genderColumn?: string;
  conditionColumn?: string;
}