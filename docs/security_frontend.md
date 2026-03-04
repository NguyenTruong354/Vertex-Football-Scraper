# Vertex Football - Frontend Security Architecture

This document outlines the essential security and anti-scraping measures that must be implemented in the React (Vite/Next.js) Frontend Dashboard to protect our exclusive football data (Live Insight, Match Story, Player Trend, Live Coords) and ensure the integrity of user accounts.

---

## 1. Authentication & Token Management (JWT)
**Goal:** Prevent unauthorized access and protect user identities.
*   **No `localStorage` for Access Tokens:** Never store the JWT Access Token in `localStorage` or `sessionStorage` where it is vulnerable to Cross-Site Scripting (XSS).
*   **HttpOnly Cookies for Refresh Tokens:** The Spring Boot API should return a long-lived Refresh Token as a secure, `HttpOnly`, `SameSite=Strict` cookie. The React app cannot read this cookie via JavaScript.
*   **In-Memory Access Tokens:** The short-lived JWT Access Token (e.g., valid for 15 mins) should be kept in the React state/memory (or injected into Axios interceptors) and silently refreshed via the HttpOnly cookie when needed.

## 2. API Call Protection (Anti-Scraping)
**Goal:** Stop headless browsers and automated bots from mimicking frontend API calls to scrape our data.
*   **Dynamic Request Signatures (HMAC):** For highly sensitive endpoints (like Live Match Coordinates or AI Insights), implement a request signing mechanism. The frontend generates a hash (HMAC) based on the timestamp, requested URL, and a hidden client secret before sending the request. The backend verifies the signature and timestamp (to prevent replay attacks).
*   **Fingerprinting (Device Check):** Integrate a library like FingerprintJS. Attach a unique device/browser hash to login and critical requests so the backend can rate-limit or block suspicious headless browsers running the exact same configuration thousands of times.
*   **reCAPTCHA / Turnstile Integration:** If a user requests too much data too fast (e.g., clicks "View More" on Player Stats 50 times in a minute), trigger a silent Cloudflare Turnstile or reCAPTCHA v3 check to verify human interaction before loading the next page.

## 3. Cross-Site Scripting (XSS) Prevention
**Goal:** Prevent malicious users from injecting scripts into the dashboard.
*   **React's Built-in Protection:** Always use React's default data-binding (`{data}`) which automatically sanitizes inputs. **NEVER** use `dangerouslySetInnerHTML` unless rendering strictly controlled, pre-sanitized markdown/HTML from our backend.
*   **Content Security Policy (CSP):** Configure a strict CSP header in the production build (or via the proxy/CDN) to restrict where scripts, styles, and images can be loaded from.
    ```http
    Content-Security-Policy: default-src 'self'; script-src 'self' https://trusted-cdn.com; img-src 'self' https://api.vertex-football.com;
    ```
*   **Sanitize Third-Party Data:** Even if data comes from our own backend (e.g., News RSS feeds, AI Insights), if we decide to render it as rich HTML later, use DOMPurify on the client side before rendering.

## 4. Cross-Site Request Forgery (CSRF)
**Goal:** Prevent an attacker from tricking an authenticated user into performing unwanted actions.
*   **SameSite Cookie Attribute:** Ensure all backend auth cookies use `SameSite=Strict` or `SameSite=Lax`. This prevents the browser from sending our auth cookies when a request originates from an external site.
*   **Anti-CSRF Tokens:** For state-changing operations (POST, PUT, DELETE), if using cookie-based auth, require custom headers (e.g., `X-XSRF-TOKEN`). The frontend reads the CSRF token from a non-HttpOnly cookie set by the backend and attaches it to the header of the API request.

## 5. UI/UX Abuse Prevention
**Goal:** Stop manual scraping and brute-force actions via the UI.
*   **Debouncing & Throttling API Calls:** Buttons like "Load More Data", "Refresh Live Stats", or "Search Player" must be debounced (e.g., 500ms limit) to prevent a user from spamming the backend API by clicking madly.
*   **Disable Text Selection & Right Click (Optional/Aggressive):** For specific highly valuable data tables (like the custom Player Momentum Radar), you can aggressively use CSS (`user-select: none;`) and JS to disable right-click context menus. *(Note: This stops casual copy-pasters, but not technical scrapers. Use sparingly so as not to ruin user experience).*
*   **Route Guards:** Ensure `react-router` implements strictly protected routes. A user attempting to access `/admin/controls` or `/insight/premium` without the correct JWT Role in Redux/Zustand state must be instantly bounced back to the `/login` or `/dashboard` page.

---

**Summary:** While the Backend API enforces the *hard limits* (Rate Limiting, CORS, WAF), the Frontend acts as the **First Line of Defense and Identity Manager**. Proper implementation of in-memory Tokens, strict CSP, and debounced request signing will make scraping Vertex Football an absolute nightmare for competitors.
