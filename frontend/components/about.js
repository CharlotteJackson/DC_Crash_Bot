const About = () => {
  return (
    <section className="grid grid-cols-1 lg:grid-cols-2 lg:col-gap-32 mt-16 lg:mt-32">
      <img
        src="images/traffic-light.jpg"
        alt="traffic light with walking sign"
      />
      <div className="text-center lg:text-left">
        <h1 className="font-bold text-4xl leading-tight mt-16 mb-8">
          DC Traffic Safety Monitor
        </h1>
        <p>
          Lorem ipsum dolor, sit amet consectetur adipisicing elit. Officiis,
          commodi? Explicabo dicta, dolores provident corrupti dolorum sit rerum
          voluptas aperiam esse quam porro. Dolorem sint ex sed dolor reiciendis
          quia?
        </p>
      </div>
    </section>
  );
};

export default About;
