type H3LatLonApi = {
  cellToLatLng?: (id: string) => [number, number];
  h3ToGeo?: (id: string) => [number, number];
};

export function h3ToLatLon(h3Lib: unknown, h3Id: string): [number, number] {
  const anyH3 = h3Lib as H3LatLonApi;

  if (typeof anyH3.cellToLatLng === 'function') {
    return anyH3.cellToLatLng(h3Id);
  }

  if (typeof anyH3.h3ToGeo === 'function') {
    return anyH3.h3ToGeo(h3Id);
  }

  throw new Error('No supported H3 decode method found in h3-js');
}
