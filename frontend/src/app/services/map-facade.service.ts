import { Injectable } from '@angular/core';
import * as L from 'leaflet';
import { HotspotPoint } from '../models/hotspot-point.model';
import { MapLayerContext } from '../models/map-layer-context.model';
import { HotspotMergeService } from './hotspot-merge.service';
import { HotspotVisualizationService } from './hotspot-visualization.service';

@Injectable({
  providedIn: 'root',
})
export class MapFacadeService {
  constructor(
    private readonly hotspotMergeService: HotspotMergeService,
    private readonly hotspotVisualizationService: HotspotVisualizationService
  ) {}

  destroyMap(context?: MapLayerContext): void {
    if (!context) {
      return;
    }

    context.map.off();
    context.map.remove();
  }

  initializeMap(
    containerId: string,
    globalBounds: L.LatLngBoundsExpression,
    onZoomEnd: () => void
  ): MapLayerContext {
    const map = L.map(containerId, {
      zoomControl: true,
      attributionControl: true,
      maxBounds: globalBounds,
      maxBoundsViscosity: 1.0,
      minZoom: 2,
      preferCanvas: true,
    }).fitBounds(globalBounds);

    L.tileLayer(
      'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png',
      {
        attribution:
          '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
        subdomains: 'abcd',
        noWrap: true,
        bounds: [[-90, -180], [90, 180]],
        minZoom: 0,
        maxZoom: 12,
        keepBuffer: 4,
        updateWhenIdle: false,
        updateWhenZooming: false,
        crossOrigin: true,
      } as L.TileLayerOptions
    ).addTo(map);

    map.createPane('hotspotPane');
    map.getPane('hotspotPane')!.style.zIndex = '520';

    const hotspotLayer = L.layerGroup().addTo(map);

    map.on('zoomend', onZoomEnd);

    return {
      map,
      hotspotLayer,
    };
  }

  renderHotspots(
    context: MapLayerContext,
    hotspots: HotspotPoint[],
    isAnimating: boolean,
    centerDotMinZoom: number
  ): void {
    context.hotspotLayer.clearLayers();

    const currentZoom = context.map.getZoom();
    const points = this.hotspotMergeService.mergeCoincidentHotspots(hotspots, currentZoom);

    for (const point of points) {
      const colorBase = point.originalCases ?? point.cases;
      const fillColor = this.hotspotVisualizationService.getH3Color(colorBase);

      const sizeIntensity = Math.min(1, point.cases / 50000);
      const opacityIntensity = Math.min(1, point.cases / 50000);

      const baseRadiusMeters = this.hotspotVisualizationService.getMainCircleRadiusMeters(
        point.cases,
        currentZoom
      );

      const haloOpacity = this.hotspotVisualizationService.getHaloOpacity(opacityIntensity);
      const fillOpacity = this.hotspotVisualizationService.getMainFillOpacity(fillColor, sizeIntensity);

      const haloCircle = L.circle([point.lat, point.lon], {
        pane: 'hotspotPane',
        radius: baseRadiusMeters * 1.4,
        className: 'hotspot-halo-circle',
        color: fillColor,
        weight: 0,
        fillColor,
        fillOpacity: haloOpacity,
      }).addTo(context.hotspotLayer);

      const mainCircle = L.circle([point.lat, point.lon], {
        pane: 'hotspotPane',
        radius: baseRadiusMeters,
        className: 'hotspot-main-circle',
        color: fillColor,
        weight: 1,
        opacity: 1,
        dashArray: '5, 5',
        fillColor,
        fillOpacity,
      })
        .bindPopup(this.buildHotspotPopup(point))
        .addTo(context.hotspotLayer);

      if (currentZoom >= centerDotMinZoom) {
        const centerDotRadiusPx = this.hotspotVisualizationService.getCenterDotRadiusPx(
          sizeIntensity,
          currentZoom,
          centerDotMinZoom
        );

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
        }).addTo(context.hotspotLayer);
      }

      if (isAnimating) {
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

  }

  private buildHotspotPopup(point: HotspotPoint): string {
    const merged = point.mergedCount && point.mergedCount > 1
      ? `<br/><strong>Merged Hotspots:</strong> ${point.mergedCount}`
      : '';

    return `<strong>${point.conditionDisplay}</strong><br/><strong>Cases:</strong> ${point.cases.toLocaleString()}<br/><strong>Days:</strong> ${point.daysWithCases}${merged}<br/><strong>Lat/Lon:</strong> ${point.lat.toFixed(6)}, ${point.lon.toFixed(6)}<br/><strong>H3:</strong> ${point.h3}`;
  }
}
