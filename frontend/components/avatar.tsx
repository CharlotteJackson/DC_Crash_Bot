const Avatar = ({ name, picture, href }) => {
  return (
    <div className="flex items-center">
      <img src={picture} alt={name} className="w-8 h-8 rounded-full mr-4" />
      <a href={href} target="_blank" className="hover:underline text-sm">
        {name}
      </a>
    </div>
  );
};

export default Avatar;
