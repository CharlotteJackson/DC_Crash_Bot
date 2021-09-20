class DCMap {
  constructor() {
    this.map = this.initializeMap("map");
    this.addStreetData(this.map);
    this.streetLayer;
    this.highlightedStreet;
    this.highlightStreetOnHover;
    this.resetHighlight;
  }

  /**
   * Use Leaflet to initialize a new map
   * @param {string} htmlId - HTML div ID to attach the map to
   * @returns L.Map (Leaflet Map)
   */
  initializeMap(htmlId) {
    /* Use Leaflet to initialize a new map on the provided html div */
    /* Jacob: switched zoom to 15 for testing highlight effect*/
    const map = L.map(htmlId).setView([38.9, -77.05], 16);
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
    axios
      .get("/dcmap/street_centerlines_2013_small.geojson")
      .then((response) => {
        console.log("streets GeoJSON:", response);

        this.streetLayer = L.geoJSON(response.data, {
          onEachFeature: (feature, layer) => {
            layer.on({
              click: () => {
                this.highlightStreetOnClick(layer)
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

  highlightStreetOnClick(layer) {
    layer.setStyle({
      stroke: true,
      weight: 6,
      dasharray: "",
      opacity: 0.7,
      color: "#f3e726",
    });
  }

  highlightStreetOnHover(layer) {
    layer.setStyle({
      stroke: true,
      weight: 8,
      dasharray: "",
      opacity: 0.7,
      color: "#ff5733",
    });
  }

  resetHighlight(layer) {
    console.log(L.Path.setStyle({color: "#ffffff"}))
    layer.options.color == "#ff5733" && layer.setStyle({
      weight: 5,
      color: "#3388ff",
      dashArray: "",
      fillOpacity: 1,
    })
  }
}

new DCMap();
