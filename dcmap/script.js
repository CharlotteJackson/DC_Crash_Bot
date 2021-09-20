class DCMap {
  constructor() {
    this.map = this.initializeMap("map");
    this.addStreetData(this.map);
    this.streetLayer;
    this.highlightedStreet;
    this.highlightStreetOnHover;
    this.resetHighlight;
    // stores highlighted layer
    this.highlightedLayer = "";
    this.onlyStreetsWithNamesFilter;
  }

  /**
   * Use Leaflet to initialize a new map
   * @param {string} htmlId - HTML div ID to attach the map to
   * @returns L.Map (Leaflet Map)
   */
  initializeMap(htmlId) {
    /* Use Leaflet to initialize a new map on the provided html div */
    const map = L.map(htmlId).setView([38.9, -77.05], 15);
    this.addBaseMap(map);
    return map;
  }

  /**
   * Add street data to the map
   * @param {L.Map} map - Leaflet map
   */
  addStreetData(map) {
    // TODO: See if we can find a better roads layer eventually
    axios
      .get("/dcmap/street_centerlines_2013_small.geojson")
      .then((response) => {
        this.streetLayer = L.geoJSON(response.data, {
          filter: this.onlyStreetsWithNamesFilter,
          onEachFeature: (feature, layer) => {
            layer.on({
              click: () => {
                this.highlightStreetOnClick(layer);
              },
              mouseover: () => {
                // Highlights road on mouse hover
                this.highlightStreetOnHover(layer);
              },
              // Removes highlight when no longer hover
              mouseout: () => this.resetHighlight(layer),
            });
          },
        });

        this.streetLayer.bindPopup((layer) => {
          return `Street Name: ${layer.feature.properties.ST_NAME}`;
        });
        map.addLayer(this.streetLayer);
      })
      .catch((error) => {
        console.error("Error in axios promise:");
        console.error(error);
      });
  }

  /**
   * Add a basemap (the reference map behind the data)
   * More basemaps can be chosen from: http://leaflet-extras.github.io/leaflet-providers/preview/
   * @param {L.Map} map - Leaflet map
   */
  addBaseMap(map) {
    const Esri_WorldGrayCanvas = L.tileLayer(
      "https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}",
      {
        attribution: "Tiles &copy; Esri &mdash; Esri, DeLorme, NAVTEQ",
        maxZoom: 16,
      }
    );
    Esri_WorldGrayCanvas.addTo(map);
  }

  onlyStreetsWithNamesFilter(feature) {
    if (!!feature.properties.ST_NAME) return true;
  }

  highlightStreetOnClick(layer) {
    layer.setStyle({
      stroke: true,
      weight: 6,
      dasharray: "",
      opacity: 0.7,
      color: "#f3e726",
    });
    /**
     * Stores clicked street layer to later be reset to normal color after another street is clicked
     * **/
    if (this.highlightedLayer) {
      this.highlightedLayer.setStyle({
        weight: 5,
        color: "#3388ff",
        dashArray: "",
        fillOpacity: 1,
      });
      this.highlightedLayer = layer;
    } else {
      this.highlightedLayer = layer;
    }
  }

  highlightStreetOnHover(layer) {
    // If street layer is normal color, highlight street
    layer.options.color == "#3388ff" &&
      layer.setStyle({
        stroke: true,
        weight: 8,
        dasharray: "",
        opacity: 0.7,
        color: "#ff5733",
      });
  }

  resetHighlight(layer) {
    // if street layer is highlighted, return layer to normal color
    layer.options.color == "#ff5733" &&
      layer.setStyle({
        weight: 5,
        color: "#3388ff",
        dashArray: "",
        fillOpacity: 1,
      });
  }
}

new DCMap();
