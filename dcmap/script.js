class DCMap {
  constructor() {
    this.map = this.initializeMap('map');
    this.addStreetData(this.map);
    this.streetLayer;
    this.highlightedStreet;
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
    // TODO: filter out roads that do not have names: https://stackoverflow.com/questions/37023790/leaflet-create-layers-from-geojson-properties
    // TODO: See if we can find a better roads layer eventually
    axios.get('/dcmap/street_centerlines_2013_small.geojson')
      .then((response) => {
        console.log("streets GeoJSON:", response);

        this.streetLayer = L.geoJSON(response.data, {
          onEachFeature: (feature, layer) => {
            layer.on({
              click: () => {
                /* On Road Click */
                // TODO: Highlight the road that was clicked
                console.log("Layer clicked!", layer)
                // this.highlightedStreet = 'the layer above'
                // this.map.removeLayer(this.streetLayer)
              }
            });
          }
        });

        this.streetLayer.bindPopup((layer) => {
          return `Street Name: ${layer.feature.properties.ST_NAME}`
        })
        map.addLayer(this.streetLayer);
      })
      .catch((error) => {
        console.error("Error in axios promise:")
        console.error(error);
      })
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