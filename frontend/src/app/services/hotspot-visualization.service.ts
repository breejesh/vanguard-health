import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root',
})
export class HotspotVisualizationService {
  private readonly maxCaseScale = 50000;

  getMainCircleRadiusMeters(cases: number, zoom: number): number {
    const sizeIntensity = Math.min(1, cases / this.maxCaseScale);
    const zoomDelta = Math.max(0, zoom - 2);
    const zoomScale = Math.exp(-0.62 * zoomDelta);
    const radiusByZoom = (120000 + sizeIntensity * 180000) * zoomScale;
    const minRadiusMeters = 900 + 4100 / (1 + Math.exp(0.9 * (zoom - 6)));
    return Math.max(minRadiusMeters, radiusByZoom) * 1.3;
  }

  getMainFillOpacity(fillColor: string, sizeIntensity: number): number {
    const luminance = this.getHexLuminance(fillColor);
    const baseOpacity = 0.22 + sizeIntensity * 0.2;
    const lightColorBoost = 0.28 * luminance;
    const opacity = baseOpacity + lightColorBoost;
    return Math.max(0.24, Math.min(0.64, opacity));
  }

  getHaloOpacity(opacityIntensity: number): number {
    return Math.max(0.2, opacityIntensity * 0.15);
  }

  getCenterDotRadiusPx(sizeIntensity: number, currentZoom: number, minZoom: number): number {
    const zoomBoost = Math.max(0, currentZoom - minZoom);
    return Math.min(5.5, 2.2 + sizeIntensity * 1.6 + zoomBoost * 0.25);
  }

  getBaseMergeThresholdKm(currentZoom: number): number {
    if (currentZoom <= 2) return 900;
    if (currentZoom <= 3) return 650;
    if (currentZoom <= 5) return 240;
    if (currentZoom <= 7) return 50;
    if (currentZoom <= 9) return 15;
    return 5;
  }

  getMaxMergeThresholdKm(currentZoom: number): number {
    const maxRadiusKm = this.getMainCircleRadiusMeters(this.maxCaseScale, currentZoom) / 1000;
    return Math.max(this.getBaseMergeThresholdKm(currentZoom), maxRadiusKm * 2);
  }

  getH3Color(cases: number): string {
    if (cases < 2000) return '#ffff66';
    if (cases < 4000) return '#ffee00';
    if (cases < 6000) return '#ffcc00';
    if (cases < 8000) return '#ffaa00';
    if (cases < 12000) return '#ff8800';
    if (cases < 16000) return '#ff6600';
    if (cases < 25000) return '#ff3300';
    return '#cc0000';
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
}
