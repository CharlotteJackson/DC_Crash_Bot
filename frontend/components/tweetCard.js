const TweetCard = ({ text }) => {
  const textArr = text.split('https:');
  const content = `${textArr[0]}...`;
  const href = `https:${textArr[1]}`;

  return (
    <div className="max-w-sm rounded overflow-hidden shadow-md">
      <div className="px-6 py-4">
        <p className="text-gray-700 text-base">{content}</p>
      </div>
      <div className="px-6 pt-4 pb-2">
        <span className="inline-block bg-gray-200 rounded-full px-3 py-1 text-sm font-semibold text-gray-700 mr-2 mb-2">
          <a href={href} target="_blank">learn more</a>
        </span>
      </div>
    </div>
  );
};

export default TweetCard;
