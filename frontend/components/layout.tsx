import Meta from './meta';
import Header from './header';
import Footer from './footer';

const Layout = ({ children }) => (
  <>
    <Meta />
    <div className="min-h-screen">
      <Header />
      <main>{children}</main>
    </div>
    <Footer />
  </>
);

export default Layout;
