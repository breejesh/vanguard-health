import { Injectable } from '@angular/core';
import * as duckdb from '@duckdb/duckdb-wasm';
import { environment } from '../../environments/environment';
import { ConditionParquetSchema } from '../models/parquet/condition-parquet-schema.model';
import { LocalDuckDbBundle } from '../models/parquet/local-duckdb-bundle.model';
import { ParquetAggregatedRow } from '../models/parquet/parquet-aggregated-row.model';
import { ParquetConditionOption } from '../models/parquet/parquet-condition-option.model';
import { ParquetDailyRow } from '../models/parquet/parquet-daily-row.model';
import { ParquetMetadataSnapshot } from '../models/parquet/parquet-metadata-snapshot.model';
import { ParquetQueryFilters } from '../models/parquet/parquet-query-filters.model';

export type { ParquetAggregatedRow } from '../models/parquet/parquet-aggregated-row.model';
export type { ParquetConditionOption } from '../models/parquet/parquet-condition-option.model';
export type { ParquetDailyRow } from '../models/parquet/parquet-daily-row.model';
export type { ParquetMetadataSnapshot } from '../models/parquet/parquet-metadata-snapshot.model';
export type { ParquetQueryFilters } from '../models/parquet/parquet-query-filters.model';

type DuckDbBundleSource = 'local' | 'cdn';
type DuckDbBundleCandidates = Record<
  string,
  {
    mainModule: string;
    mainWorker: string;
    pthreadWorker?: string | null;
  }
>;

@Injectable({
  providedIn: 'root',
})
export class ParquetDataService {
  private db?: duckdb.AsyncDuckDB;
  private connection?: duckdb.AsyncDuckDBConnection;
  private initPromise?: Promise<void>;
  private readonly schemaByCondition = new Map<string, ConditionParquetSchema>();
  private readonly registeredParquetAliases = new Set<string>();
  private readonly metadataAliasByPath = new Map<string, string>();

  constructor() {}

  async loadMetadata(): Promise<ParquetMetadataSnapshot> {
    return this.loadMetadataFromParquet();
  }

  private async loadMetadataFromParquet(): Promise<ParquetMetadataSnapshot> {
    const [conditionRows, h3Rows] = await Promise.all([
      this.readParquetRows(this.getParquetFileName('conditionsParquet'), 'conditions_meta'),
      this.readParquetRows(this.getParquetFileName('h3ReferenceParquet'), 'h3_meta'),
    ]);

    const conditionOptions: ParquetConditionOption[] = conditionRows
      .map((row) => ({
        code: this.getStringValue(row, ['condition_code', 'condition']) || '',
        display:
          this.getStringValue(row, ['condition_display', 'display_name']) ||
          this.getStringValue(row, ['condition_code', 'condition']) ||
          '',
        patientCount: this.getNumberValue(row, ['patient_count', 'case_count']),
      }))
      .filter((row) => row.code.length > 0);

    const firstConditionRow = conditionRows[0] ?? {};
    const firstH3Row = h3Rows[0] ?? {};

    const ageGroups = this.parseJsonArray(
      this.getStringValue(firstConditionRow, ['age_groups_json'])
    );
    const genders = this.parseJsonArray(
      this.getStringValue(firstConditionRow, ['genders_json'])
    );

    const h3Reference: Record<string, { latitude: number; longitude: number }> = {};
    for (const row of h3Rows) {
      const h3 = this.getStringValue(row, ['h3', 'h3_id']);
      if (!h3) {
        continue;
      }

      h3Reference[h3] = {
        latitude: this.getNumberValue(row, ['latitude', 'lat']),
        longitude: this.getNumberValue(row, ['longitude', 'lng', 'lon']),
      };
    }

    const totalCases = conditionOptions.reduce((sum, condition) => sum + condition.patientCount, 0);
    const lastUpdatedIso =
      this.getStringValue(firstConditionRow, ['generated_at']) ||
      this.getStringValue(firstH3Row, ['generated_at']) ||
      new Date().toISOString();

    const totalConditions = this.getNumberValue(firstConditionRow, ['total_unique']) || conditionOptions.length;
    const totalCells = this.getNumberValue(firstH3Row, ['total_unique']) || Object.keys(h3Reference).length;

    return {
      lastUpdatedIso,
      totalConditions,
      totalCases,
      totalPatients: totalCases,
      totalCells,
      ageGroups,
      genders,
      conditions: conditionOptions,
      h3Reference,
    };
  }

  async queryAggregatedHotspots(
    filters: ParquetQueryFilters
  ): Promise<{ rows: ParquetAggregatedRow[]; totalCases: number; activeDays: number }> {
    if (filters.ageGroups.length === 0 || filters.genders.length === 0) {
      return { rows: [], totalCases: 0, activeDays: 0 };
    }

    const schema = await this.ensureConditionSchema(filters.conditionCode);
    const conn = this.requireConnection();
    const source = `read_parquet(${this.sqlLiteral(schema.alias)})`;
    const whereClause = this.buildWhereClause(schema, filters);
    const caseExpr = `CAST(${schema.caseColumn} AS BIGINT)`;

    const hotspotResult = await conn.query(`
      SELECT
        ${schema.h3Column} AS h3,
        SUM(${caseExpr}) AS case_count,
        MAX(${schema.dateColumn}) AS latest_date,
        COUNT(DISTINCT ${schema.dateColumn}) AS days_with_cases
      FROM ${source}
      ${whereClause}
      GROUP BY ${schema.h3Column}
      HAVING SUM(${caseExpr}) > 0
    `);

    const statsResult = await conn.query(`
      SELECT
        COALESCE(SUM(${caseExpr}), 0) AS total_cases,
        COUNT(DISTINCT ${schema.dateColumn}) AS active_days
      FROM ${source}
      ${whereClause}
    `);

    const rows = this.tableToRows(hotspotResult).map((row) => ({
      h3: this.toString(row['h3']),
      caseCount: this.toNumber(row['case_count']),
      latestDate: this.toString(row['latest_date']),
      daysWithCases: this.toNumber(row['days_with_cases']),
    }));

    const statsRow = this.tableToRows(statsResult)[0] ?? {};

    return {
      rows,
      totalCases: this.toNumber(statsRow['total_cases']),
      activeDays: this.toNumber(statsRow['active_days']),
    };
  }

  async queryDailySeries(filters: ParquetQueryFilters): Promise<ParquetDailyRow[]> {
    if (filters.ageGroups.length === 0 || filters.genders.length === 0) {
      return [];
    }

    const schema = await this.ensureConditionSchema(filters.conditionCode);
    const conn = this.requireConnection();
    const source = `read_parquet(${this.sqlLiteral(schema.alias)})`;
    const whereClause = this.buildWhereClause(schema, filters);
    const caseExpr = `CAST(${schema.caseColumn} AS BIGINT)`;

    const result = await conn.query(`
      SELECT
        ${schema.dateColumn} AS date_key,
        ${schema.h3Column} AS h3,
        SUM(${caseExpr}) AS case_count
      FROM ${source}
      ${whereClause}
      GROUP BY ${schema.dateColumn}, ${schema.h3Column}
      HAVING SUM(${caseExpr}) > 0
      ORDER BY ${schema.dateColumn}
    `);

    return this.tableToRows(result).map((row) => ({
      dateKey: this.toString(row['date_key']),
      h3: this.toString(row['h3']),
      caseCount: this.toNumber(row['case_count']),
    }));
  }

  async getLatestDataDate(conditionCode: string): Promise<string | null> {
    const schema = await this.ensureConditionSchema(conditionCode);
    const conn = this.requireConnection();
    const source = `read_parquet(${this.sqlLiteral(schema.alias)})`;

    const result = await conn.query(`
      SELECT CAST(MAX(${schema.dateColumn}) AS VARCHAR) AS latest_date
      FROM ${source}
    `);

    const row = this.tableToRows(result)[0] ?? {};
    const latestDate = this.getStringValue(row, ['latest_date']);
    return latestDate || null;
  }

  private async ensureInitialized(): Promise<void> {
    if (!this.initPromise) {
      this.initPromise = this.initializeDuckDb();
    }
    await this.initPromise;
  }

  private async initializeDuckDb(): Promise<void> {
    const candidates = await this.getBundleCandidates();

    if (candidates.length === 0) {
      throw new Error('No DuckDB bundle candidates available. Check environment duckdb bundle configuration.');
    }

    let lastError: unknown;
    for (const candidate of candidates) {
      try {
        await this.instantiateBundle(candidate.bundle, candidate.name);
        this.connection = await this.requireDb().connect();
        return;
      } catch (error) {
        console.warn(`DuckDB bundle '${candidate.name}' failed to initialize.`, error);
        lastError = error;
      }
    }

    throw new Error(`Unable to load DuckDB-WASM bundle: ${String(lastError ?? 'unknown error')}`);
  }

  private async getBundleCandidates(): Promise<Array<{ name: string; bundle: LocalDuckDbBundle }>> {
    const priorities = this.getBundlePriority();
    const candidates: Array<{ name: string; bundle: LocalDuckDbBundle }> = [];

    for (const source of priorities) {
      if (source === 'local') {
        const localBundle = await this.selectBundle(this.getLocalDuckDbBundles());
        if (localBundle) {
          candidates.push({ name: 'local', bundle: localBundle });
        }
      }

      if (source === 'cdn') {
        const cdnBundles = this.getCdnBundles();
        if (cdnBundles) {
          const cdnBundle = await this.selectBundle(cdnBundles);
          if (cdnBundle) {
            candidates.push({ name: 'cdn', bundle: cdnBundle });
          }
        }
      }
    }

    return candidates;
  }

  private getBundlePriority(): DuckDbBundleSource[] {
    const raw = (environment as unknown as { duckdbBundlePriority?: unknown }).duckdbBundlePriority;
    if (!Array.isArray(raw)) {
      return ['local', 'cdn'];
    }

    const parsed = raw
      .map((entry) => String(entry).toLowerCase())
      .filter((entry): entry is DuckDbBundleSource => entry === 'local' || entry === 'cdn');

    return parsed.length > 0 ? parsed : ['local', 'cdn'];
  }

  private getCdnBundles(): DuckDbBundleCandidates | null {
    const duckdbAny = duckdb as unknown as { getJsDelivrBundles?: () => DuckDbBundleCandidates };
    if (typeof duckdbAny.getJsDelivrBundles !== 'function') {
      return null;
    }
    return duckdbAny.getJsDelivrBundles();
  }

  private async selectBundle(bundles: DuckDbBundleCandidates): Promise<LocalDuckDbBundle | null> {
    try {
      const selected = await duckdb.selectBundle(bundles as unknown as duckdb.DuckDBBundles);
      const selectedAny = selected as unknown as {
        mainModule?: string;
        mainWorker?: string;
        pthreadWorker?: string | null;
      };

      if (!selectedAny.mainModule || !selectedAny.mainWorker) {
        return null;
      }

      return {
        mainModule: selectedAny.mainModule,
        mainWorker: selectedAny.mainWorker,
        pthreadWorker: selectedAny.pthreadWorker ?? null,
      };
    } catch {
      return null;
    }
  }

  private async instantiateBundle(bundle: LocalDuckDbBundle, bundleName: string): Promise<void> {
    if (!bundle.mainWorker || !bundle.mainModule) {
      throw new Error(`DuckDB bundle '${bundleName}' is missing worker or wasm module.`);
    }

    const worker = this.createDuckDbWorker(bundle.mainWorker);
    const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);

    try {
      await this.withTimeout(
        db.instantiate(bundle.mainModule, bundle.pthreadWorker),
        15000,
        `DuckDB bundle '${bundleName}' initialization timed out.`
      );
      this.db = db;
    } catch (error) {
      worker.terminate();
      throw error;
    }
  }

  private createDuckDbWorker(workerUrl: string): Worker {
    const normalizedWorkerUrl = new URL(workerUrl, document.baseURI).toString();

    // Some duckdb worker builds reference _setThrew before defining it.
    // Predefine a no-op shim before loading the actual worker script.
    const bootstrapScript = [
      'self._setThrew = self._setThrew || function () {};',
      `importScripts(${JSON.stringify(normalizedWorkerUrl)});`,
    ].join('\n');

    const bootstrapBlob = new Blob([bootstrapScript], { type: 'application/javascript' });
    const bootstrapBlobUrl = URL.createObjectURL(bootstrapBlob);

    try {
      return new Worker(bootstrapBlobUrl);
    } finally {
      URL.revokeObjectURL(bootstrapBlobUrl);
    }
  }

  private async withTimeout<T>(promise: Promise<T>, timeoutMs: number, timeoutMessage: string): Promise<T> {
    return await new Promise<T>((resolve, reject) => {
      const timeoutHandle = setTimeout(() => {
        reject(new Error(timeoutMessage));
      }, timeoutMs);

      promise.then(
        (value) => {
          clearTimeout(timeoutHandle);
          resolve(value);
        },
        (error) => {
          clearTimeout(timeoutHandle);
          reject(error);
        }
      );
    });
  }

  private getLocalDuckDbBundles(): DuckDbBundleCandidates {
    const localBundles = (environment as unknown as {
      duckdbBundles?: {
        mvp?: LocalDuckDbBundle;
        eh?: LocalDuckDbBundle;
      };
    }).duckdbBundles;

    const bundles: DuckDbBundleCandidates = {};

    if (localBundles?.mvp?.mainModule && localBundles.mvp.mainWorker) {
      bundles['mvp'] = {
        mainModule: this.resolveAssetUrl(localBundles.mvp.mainModule),
        mainWorker: this.resolveAssetUrl(localBundles.mvp.mainWorker),
      };
    }

    if (localBundles?.eh?.mainModule && localBundles.eh.mainWorker) {
      bundles['eh'] = {
        mainModule: this.resolveAssetUrl(localBundles.eh.mainModule),
        mainWorker: this.resolveAssetUrl(localBundles.eh.mainWorker),
      };
    }

    return bundles;
  }

  private resolveAssetUrl(relativePath: string): string {
    return new URL(relativePath, document.baseURI).toString();
  }

  private async ensureConditionSchema(conditionCode: string): Promise<ConditionParquetSchema> {
    await this.ensureInitialized();

    const cached = this.schemaByCondition.get(conditionCode);
    if (cached) {
      return cached;
    }

    const db = this.requireDb();
    const conn = this.requireConnection();
    const alias = this.aliasForCondition(conditionCode);

    if (!this.registeredParquetAliases.has(alias)) {
      await this.registerParquetBuffer(alias, this.buildConditionParquetUrl(conditionCode));
      this.registeredParquetAliases.add(alias);
    }

    const describeResult = await conn.query(
      `DESCRIBE SELECT * FROM read_parquet(${this.sqlLiteral(alias)})`
    );

    const columnNames = new Set<string>();
    for (const row of this.tableToRows(describeResult)) {
      const columnName = this.extractColumnName(row);
      if (columnName) {
        columnNames.add(columnName.toLowerCase());
      }
    }

    const schema: ConditionParquetSchema = {
      alias,
      dateColumn: this.requiredColumn(columnNames, ['date_key', 'date']),
      h3Column: this.requiredColumn(columnNames, ['h3']),
      caseColumn: this.requiredColumn(columnNames, ['case_count', 'casecount']),
      ageColumn: this.optionalColumn(columnNames, ['age_group', 'agegroup']),
      genderColumn: this.optionalColumn(columnNames, ['gender']),
      conditionColumn: this.optionalColumn(columnNames, ['condition_code', 'condition']),
    };

    this.schemaByCondition.set(conditionCode, schema);
    return schema;
  }

  private buildWhereClause(schema: ConditionParquetSchema, filters: ParquetQueryFilters): string {
    const clauses: string[] = [
      `${schema.dateColumn} >= ${this.sqlLiteral(filters.startDate)}`,
      `${schema.dateColumn} <= ${this.sqlLiteral(filters.endDate)}`,
    ];

    if (schema.conditionColumn) {
      clauses.push(`${schema.conditionColumn} = ${this.sqlLiteral(filters.conditionCode)}`);
    }

    if (schema.ageColumn && filters.ageGroups.length > 0) {
      clauses.push(`${schema.ageColumn} IN (${filters.ageGroups.map((value) => this.sqlLiteral(value)).join(', ')})`);
    }

    if (schema.genderColumn && filters.genders.length > 0) {
      clauses.push(`${schema.genderColumn} IN (${filters.genders.map((value) => this.sqlLiteral(value)).join(', ')})`);
    }

    return clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
  }

  private requiredColumn(columns: Set<string>, candidates: string[]): string {
    const column = this.optionalColumn(columns, candidates);
    if (!column) {
      throw new Error(`Missing required parquet column: ${candidates.join(' or ')}`);
    }
    return column;
  }

  private optionalColumn(columns: Set<string>, candidates: string[]): string | undefined {
    for (const candidate of candidates) {
      if (columns.has(candidate.toLowerCase())) {
        return candidate.toLowerCase();
      }
    }
    return undefined;
  }

  private extractColumnName(row: Record<string, unknown>): string | undefined {
    const raw = row['column_name'] ?? row['name'] ?? row['columnName'];
    if (typeof raw === 'string' && raw.trim().length > 0) {
      return raw.trim();
    }
    return undefined;
  }

  private tableToRows(table: { toArray: () => unknown[] }): Array<Record<string, unknown>> {
    return table.toArray().map((row) => {
      if (row && typeof row === 'object') {
        const candidate = row as { toJSON?: () => unknown };
        if (typeof candidate.toJSON === 'function') {
          const jsonRow = candidate.toJSON();
          if (jsonRow && typeof jsonRow === 'object') {
            return jsonRow as Record<string, unknown>;
          }
        }
        return row as Record<string, unknown>;
      }
      return {};
    });
  }

  private aliasForCondition(conditionCode: string): string {
    return `condition_${this.sanitizeConditionCode(conditionCode)}.parquet`;
  }

  private buildConditionParquetUrl(conditionCode: string): string {
    const fileName = this.getConditionParquetTemplate().replace(
      '{condition}',
      this.sanitizeConditionCode(conditionCode)
    );
    return this.buildGoldUrl(fileName);
  }

  private async readParquetRows(
    relativePath: string,
    aliasPrefix: string
  ): Promise<Array<Record<string, unknown>>> {
    await this.ensureInitialized();

    const filePath = this.buildGoldUrl(relativePath);
    let alias = this.metadataAliasByPath.get(filePath);

    if (!alias) {
      alias = `${aliasPrefix}_${this.metadataAliasByPath.size}.parquet`;
      await this.registerParquetBuffer(alias, filePath);
      this.metadataAliasByPath.set(filePath, alias);
    }

    const conn = this.requireConnection();
    const result = await conn.query(`SELECT * FROM read_parquet(${this.sqlLiteral(alias)})`);
    return this.tableToRows(result);
  }

  private sanitizeConditionCode(conditionCode: string): string {
    return conditionCode.replace(/\//g, '_').replace(/ /g, '_');
  }

  private async registerParquetBuffer(alias: string, fileUrl: string): Promise<void> {
    const db = this.requireDb();
    const response = await fetch(fileUrl, { cache: 'no-store' });

    if (!response.ok) {
      throw new Error(`Failed to fetch parquet '${fileUrl}' (HTTP ${response.status}).`);
    }

    const bytes = new Uint8Array(await response.arrayBuffer());
    if (bytes.length < 4 || bytes[0] !== 0x50 || bytes[1] !== 0x41 || bytes[2] !== 0x52 || bytes[3] !== 0x31) {
      throw new Error(`File '${fileUrl}' is not a valid parquet payload (missing PAR1 header).`);
    }

    await db.registerFileBuffer(alias, bytes);
  }

  private buildGoldUrl(relativePath: string): string {
    const normalizedPath = relativePath.replace(/^\/+/, '');
    return new URL(normalizedPath, `${this.getParquetBaseUrl()}/`).toString();
  }

  private getParquetFileName(
    key: 'conditionsParquet' | 'h3ReferenceParquet'
  ): string {
    const value = environment.parquetFiles[key];
    if (!value || typeof value !== 'string') {
      throw new Error(`Parquet file config '${key}' is missing.`);
    }
    return value;
  }

  private getConditionParquetTemplate(): string {
    const template = environment.parquetFiles.conditionParquetTemplate;
    if (!template || typeof template !== 'string' || !template.includes('{condition}')) {
      throw new Error("Parquet condition file template is missing or invalid. Expected '{condition}' placeholder.");
    }
    return template;
  }

  private getParquetBaseUrl(): string {
    const sourceKey = environment.parquetSource as keyof typeof environment.parquetSources;
    const sourceConfig = environment.parquetSources[sourceKey];

    if (!sourceConfig) {
      throw new Error(`Parquet source '${String(environment.parquetSource)}' is not configured.`);
    }

    const rawBaseUrl = sourceConfig.baseUrl;

    if (!rawBaseUrl) {
      throw new Error('Parquet base URL is not configured.');
    }

    return new URL(`${rawBaseUrl.replace(/\/+$/, '')}/`, document.baseURI)
      .toString()
      .replace(/\/+$/, '');
  }

  private sqlLiteral(value: string): string {
    return `'${value.replace(/'/g, "''")}'`;
  }

  private parseJsonArray(raw: string): string[] {
    if (!raw) {
      return [];
    }

    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        return parsed.map((value) => this.toString(value)).filter((value) => value.length > 0);
      }
    } catch {
      return [];
    }

    return [];
  }

  private getStringValue(row: Record<string, unknown>, keys: string[]): string {
    for (const key of keys) {
      const value = row[key];
      if (typeof value === 'string' && value.trim().length > 0) {
        return value.trim();
      }
      if (value !== null && value !== undefined && typeof value !== 'object') {
        return String(value);
      }
    }

    return '';
  }

  private getNumberValue(row: Record<string, unknown>, keys: string[]): number {
    for (const key of keys) {
      const value = row[key];
      const numberValue = this.toNumber(value);
      if (numberValue !== 0 || value === 0 || value === '0' || value === 0n) {
        return numberValue;
      }
    }

    return 0;
  }

  private toNumber(value: unknown): number {
    if (typeof value === 'bigint') {
      return Number(value);
    }
    const parsed = Number(value ?? 0);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  private toString(value: unknown): string {
    if (typeof value === 'string') {
      return value;
    }
    if (value === null || value === undefined) {
      return '';
    }
    return String(value);
  }

  private requireDb(): duckdb.AsyncDuckDB {
    if (!this.db) {
      throw new Error('DuckDB has not been initialized.');
    }
    return this.db;
  }

  private requireConnection(): duckdb.AsyncDuckDBConnection {
    if (!this.connection) {
      throw new Error('DuckDB connection has not been initialized.');
    }
    return this.connection;
  }
}
