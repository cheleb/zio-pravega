// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

const baseUrl = "/zio-pravega/"

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'ZIO Pravega',
  tagline: 'Pravega and ZIO together',
  url: 'https://cheleb.github.io',
  baseUrl: baseUrl,
  onBrokenLinks: 'error',
  onBrokenMarkdownLinks: 'throw',
  favicon: 'img/favicon.ico',
  organizationName: 'cheleb', // Usually your GitHub org/user name.
  projectName: 'zio-pravega', // Usually your repo name.
  
  
  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {

          path: "../zio-pravega-docs/target/mdoc",
          sidebarPath: require.resolve('./sidebars.js'),
          // Please change this to your repo.
          
          //editUrl: 'https://github.com/cheleb/zio-pravega/tree/master/docs/',
        },
        blog: {
          showReadingTime: true,
          // Please change this to your repo.
          editUrl:
            'https://github.com/facebook/docusaurus/tree/main/packages/create-docusaurus/templates/shared/',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: 'Pravega',
        logo: {
          alt: 'Pravega Logo',
          src: 'img/pravega-loading.gif',
        },
        items: [
          {
            type: 'doc',
            docId: 'intro',
            position: 'left',
            label: 'Docs',
          },
          {to: '/blog', label: 'Blog', position: 'left'},
          {
            href: 'https://cheleb.github.io/zio-pravega/api/index.html',
            label: 'API',
            position: 'right'
          },
          {
            href: 'https://github.com/cheleb/zio-pravega',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Docs',
                to: '/docs/intro',
              },
            ],
          },
          {
            title: 'Community',
            items: [
//              {
//                label: 'Stack Overflow',
//                href: 'https://stackoverflow.com/questions/tagged/docusaurus',
//              },
//              {
//                label: 'Discord',
//                href: 'https://discordapp.com/invite/docusaurus',
//              },
              {
                label: 'Twitter',
                href: 'https://twitter.com/oNouguier',
              },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'Blog',
                to: '/blog',
              },
              {
                label: 'GitHub',
                href: 'https://github.com/cheleb/zio-pravega',
              },
            ],
          },
        ],
        copyright: `Copyleft © ${new Date().getFullYear()} World, Inc. Built with Docusaurus.`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
        additionalLanguages: ["java", "scala"]        
      },
    }),
};

module.exports = config;
