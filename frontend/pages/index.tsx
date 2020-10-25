import Head from 'next/head';
import Container from '../components/container';
import Layout from '../components/layout';

export default function Home() {
  return (
    <Layout>
      <Head>
        <title>DC Crash Bot</title>
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <Container>
        <h1>Hello World!</h1>
      </Container>
    </Layout>
  );
}
