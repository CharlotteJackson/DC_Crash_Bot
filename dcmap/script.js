
class DCMap {
  constructor() {
    this.map = this.initializeMap('map');
    this.addStreetData(this.map);
  }

  /**
   * Use Leaflet to initialize a new map
   * @param {string} htmlId - HTML div ID to attach the map to
   * @returns L.Map (Leaflet Map)
   */
  initializeMap(htmlId) {
    /* Use Leaflet to initialize a new map on the provided html div */
    const map = L.map(htmlId).setView([38.9, -77.05], 11);
    this.addBaseMap(map);
    return map;
  }

  /**
   * Add street data to the map
   * @param {L.Map} map - Leaflet map
   */
  addStreetData(map) {
    // TODO: Use axios to load this geojson street layer: https://opendata.arcgis.com/datasets/e8299c86b4014f109fedd7e95ae20d52_61.geojson
  }

  /**
   * Add a basemap (the reference map behind the data)
   * More basemaps can be chosen from: http://leaflet-extras.github.io/leaflet-providers/preview/
   * @param {L.Map} map - Leaflet map
   */
  addBaseMap(map) {
    const Esri_WorldGrayCanvas = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}', {
      attribution: 'Tiles &copy; Esri &mdash; Esri, DeLorme, NAVTEQ',
      maxZoom: 16
    });
    Esri_WorldGrayCanvas.addTo(map);
  }

}

new DCMap();
