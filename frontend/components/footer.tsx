import { IoLogoSlack } from 'react-icons/io';
import { BiLinkExternal } from 'react-icons/bi';
import { FaTwitter } from 'react-icons/fa';

import Container from './container';
import Avatar from './avatar';

const SocialIcon = ({ href, children }) => (
  <li className="mr-3">
    <a
      href={href}
      target="_blank"
      className="inline-block text-lg px-2 py-2 border border-solid border-black rounded-full hover:text-green-700 hover:border-green-700">
      {children}
    </a>
  </li>
);

const Footer = () => {
  return (
    <footer className="bg-gray-300 py-12">
      <Container>
        <div className="flex justify-around">
          <div className="flex flex-col items-center">
            <p className="mb-4">
              Made with{' '}
              <span role="img" aria-label="heart">
                💚
              </span>{' '}
              by Code for DC
            </p>
            <ul className="flex">
              <SocialIcon href="https://codefordc.org/">
                <BiLinkExternal />
              </SocialIcon>
              <SocialIcon href="https://codefordc.slack.com/join/shared_invite/enQtOTAwOTEzMDA1MTU5LWRjNjIyOWMzNDBlN2FhZjJhZWIwODAwNjAzODg0YjllZmMzNjM0MjViMmFmOWFhYTE2YjZhNGYyZmVmN2RmNzI#/">
                <IoLogoSlack />
              </SocialIcon>
              <SocialIcon href="https://twitter.com/codefordc">
                <FaTwitter />
              </SocialIcon>
            </ul>
          </div>
          <ul className="flex flex-col space-y-3">
            <li>
              <Avatar
                name="CharlotteJackson"
                picture="https://avatars3.githubusercontent.com/u/44366610?s=400&v=4"
                href="https://github.com/CharlotteJackson"
              />
            </li>
            <li>
              <Avatar
                name="Banjo Obayomi"
                picture="https://avatars1.githubusercontent.com/u/696254?s=400&u=488260c17dbe0bf857caa9f642c4b6d47b664df4&v=4"
                href="https://github.com/banjtheman"
              />
            </li>
            <li>
              <Avatar
                name="Ksou"
                picture="https://avatars1.githubusercontent.com/u/3148832?s=400&v=4"
                href="https://github.com/Ksou"
              />
            </li>
            <li>
              <Avatar
                name="Shiyun Lu"
                picture="https://avatars2.githubusercontent.com/u/13009238?s=400&u=08005729d5e7048b28281a0bc5479f182b8a5288&v=4"
                href="https://www.shiyunlu.com/"
              />
            </li>
          </ul>
        </div>
      </Container>
    </footer>
  );
};

export default Footer;
