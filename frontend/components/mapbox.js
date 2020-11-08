import React, { useState } from 'react';
import ReactMapGl, { Marker, Popup } from 'react-map-gl';
import { FaCarCrash } from 'react-icons/fa';

import data from '../../data/AlertDCio_google_geo.json';
import Avatar from './avatar';

const renderedContent = (string) => `${string.split('https:')[0]}...`;
const renderedHref = (string) => `https:${string.split('https:')[1]}`;

export default function Mapbox() {
  const [viewport, setViewport] = useState({
    latitude: 38.895,
    longitude: -77.0366,
    width: '100vw',
    height: '100vh',
    zoom: 13,
  });

  const [selected, setSelected] = useState(null);

  return (
    <ReactMapGl
      {...viewport}
      onViewportChange={(nextViewport) => setViewport(nextViewport)}
      mapboxApiAccessToken="pk.eyJ1IjoibHVzaGl5dW4iLCJhIjoiY2toOGY1ZHZhMGRrdDJ5cGRpMXdpNXk5eSJ9.0JGB-ILi1qPJABJxdPTicg">
      {data.map((tweet) => (
        <Marker
          key={tweet.tweet.substr(-10)}
          latitude={tweet.google_geo.lat}
          longitude={tweet.google_geo.lng}>
          <button
            onClick={(e) => {
              e.preventDefault();
              setSelected(tweet);
            }}>
            <span role="img" aria-label="automobile">
              🚗
            </span>
          </button>
        </Marker>
      ))}

      {selected && (
        <Popup
          latitude={selected.google_geo.lat}
          longitude={selected.google_geo.lng}
          dynamicPosition={true}
          onClose={() => setSelected(null)}>
          <div className="max-w-xs">
            <div>
              <Avatar
                name={selected.user_name}
                picture={selected.user_pic}
                href={null}
              />
            </div>
            <div className="px-6 py-4">
              <p className="text-gray-700 text-base">
                {renderedContent(selected.tweet)}
              </p>
            </div>
            <div className="px-6">
              <span className="inline-block bg-gray-200 rounded-full px-3 py-1 text-sm font-semibold text-gray-700 mr-2 mb-2">
                <a href={renderedHref(selected.tweet)} target="_blank">
                  learn more
                </a>
              </span>
            </div>
          </div>
        </Popup>
      )}
    </ReactMapGl>
  );
}
