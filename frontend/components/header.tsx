import Link from 'next/link';

import Container from './container';

const NavItem = ({ href, text }) => (
  <li>
    <a href={href} className="px-3 py-2 hover:bg-gray-200 rounded">
      {text}
    </a>
  </li>
);

const Header = () => (
  <header>
    <Container>
      <nav className="flex justify-between items-center py-8">
        <Link href="/">
          <a className="text-3xl">
            walk<span className="text-green-800">safe.</span>
          </a>
        </Link>
        <ul className="flex space-x-3">
          <NavItem href="/about" text="about" />
          <NavItem href="/live" text="live" />
          <NavItem href="/history" text="history" />
        </ul>
      </nav>
    </Container>
  </header>
);

export default Header;
