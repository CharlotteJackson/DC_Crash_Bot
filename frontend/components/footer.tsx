import { IoLogoSlack } from 'react-icons/io';

import Container from './container';

const Footer = () => {
  return (
    <footer>
      <Container>
        <div className="py-14 flex flex-col lg:flex-row items-center">
          <h4>
            Made with 💛 by Code for DC
          </h4>
        </div>
      </Container>
    </footer>
  );
};

export default Footer;
