import Head from 'next/head';
import Container from '../components/container';
import Layout from '../components/layout';
import About from '../components/about';

export default function Home() {
  return (
    <Layout>
      <Head>
        <title>Walk Safe DC</title>
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <Container>
        <About />
      </Container>
    </Layout>
  );
}
