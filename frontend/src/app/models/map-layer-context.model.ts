import * as L from 'leaflet';

export interface MapLayerContext {
  map: L.Map;
  hotspotLayer: L.LayerGroup;
}
