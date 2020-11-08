// for fetching data from server later
// import useSWR from 'swr';
// const fetcher = (...args) => fetch(...args).then((res) => res.json());

import data from '../data';
import TweetCard from './tweetCard';
import Map from './map';

const Live = () => {
  // const { data, error } = useSWR('/api/tweets', fetcher);
  // if (error) return <div>failed to load</div>;
  // if (!data) return <div>Loading ... </div>;

  const renderedTweets = data.map((tweet) => (
    <TweetCard key={tweet.tweet.substr(-10)} text={tweet.tweet} />
  ));

  return (
    <section className="flex mb-3 lg:flex-row-reverse mt-16 lg:mt-32">
      <div className="w-full lg:w-2/3" style={{ height: '80vh' }}>
        <Map
          isMarkerShown
          googleMapURL="https://maps.googleapis.com/maps/api/js?v=3.exp&libraries=geometry,drawing,places&key=AIzaSyCXSwfxutGMziO5ZgPvglcdV8FlI0oKqgY"
          loadingElement={<div style={{ height: `100%` }} />}
          containerElement={<div style={{ height: `100%` }} />}
          mapElement={<div style={{ height: `100%` }} />}
        />
      </div>
      <div
        className="w-full lg:w-1/3 overflow-y-auto"
        style={{ height: '80vh' }}>
        {renderedTweets}
      </div>
    </section>
  );
};

export default Live;
