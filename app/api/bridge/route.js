// Vercel Edge Bridge for Vertex Football Scraper
// Path: app/api/bridge/route.js

export const runtime = 'edge';

export async function POST(request) {
  // Security: Check X-Bridge-Token
  const token = request.headers.get('X-Bridge-Token');
  const secret = process.env.BRIDGE_SECRET;

  if (!secret || token !== secret) {
    return new Response(JSON.stringify({ error: 'Unauthorized' }), { 
      status: 401,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  try {
    const payload = await request.json();
    const targetUrl = payload.target_url;

    if (!targetUrl) {
      return new Response(JSON.stringify({ error: 'Missing target_url' }), { 
        status: 400,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    const proxyHeaders = new Headers();
    proxyHeaders.set('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
    proxyHeaders.set('Accept', 'application/json, text/plain, */*');
    proxyHeaders.set('Referer', 'https://www.sofascore.com/');

    const originResponse = await fetch(targetUrl, {
      method: 'GET',
      headers: proxyHeaders
    });

    const body = await originResponse.text();

    return new Response(body, {
      status: originResponse.status,
      headers: { 
        'Content-Type': 'application/json',
        'Cache-Control': 'no-store'
      }
    });

  } catch (error) {
    return new Response(JSON.stringify({ error: error.message }), { 
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
}
