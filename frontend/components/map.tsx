import {
  withScriptjs,
  withGoogleMap,
  GoogleMap,
  Marker,
} from 'react-google-maps';

import data from '../data';

const Map = withScriptjs(
  withGoogleMap(() => (
    <GoogleMap defaultZoom={13} defaultCenter={{ lat: 38.895, lng: -77.0366 }}>
      {data.map((tweet) => (
        <Marker
          key={tweet.tweet.substr(-10)}
          position={{ lat: tweet.google_geo.lat, lng: tweet.google_geo.lng }}
        />
      ))}
    </GoogleMap>
  ))
);

export default Map;
