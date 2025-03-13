import {defineConfig} from "umi";

export default defineConfig({
  dva: {},
  mfsu: false,
  title: 'Article.AI',
  links: [
    {id: 'theme', rel: 'stylesheet', type: 'text/css'},
    {rel: 'shortcut icon', href: '/favicon.svg'}
  ],
  routes: [
    {
      path: "/",
      component: "@/pages/MainFrame"
    }
  ],
  proxy: {
    // 平台API
    "/api": {
      target: "http://localhost:8089",
      changeOrigin: true,
      pathRewrite: {"^": ""},
      'onProxyRes': function (proxyRes, req, res) {
        proxyRes.headers['Content-Encoding'] = 'chunked';
      }
    },
  },
  esbuildMinifyIIFE: true,
  base: "/",
  outputPath: "dist",
});