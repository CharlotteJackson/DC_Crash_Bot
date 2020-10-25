import { IoLogoSlack } from 'react-icons/io';

import Container from './container';

const Footer = () => {
  return (
    <footer className="bg-linen">
      <Container>
        <div className="py-14 flex flex-col lg:flex-row items-center">
          <h4 className="text-xl lg:text-3xl tracking-tighter leading-tight text-center lg:text-left mb-10 lg:mb-0 lg:pr-4 lg:w-1/2">
            Made with 💛 by <span className="text-lemon">Code for DC</span>
          </h4>
        </div>
      </Container>
    </footer>
  );
};

export default Footer;
