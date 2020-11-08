import Head from 'next/head';
import Layout from '../components/layout';
import About from '../components/about';
import Live from '../components/live';

export default function Home() {
  return (
    <Layout>
      <Head>
        <title>Walk Safe DC</title>
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <div className="container mx-auto px-5">
        <About />
        <Live />
      </div>
    </Layout>
  );
}
