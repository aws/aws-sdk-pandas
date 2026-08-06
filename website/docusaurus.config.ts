import { themes as prismThemes } from 'prism-react-renderer';
import type { Config } from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'AWS SDK for pandas',
  tagline: 'pandas on AWS — DataFrames connected to 20+ AWS data & analytics services',
  favicon: 'img/favicon.ico',

  url: 'https://aws.github.io',
  baseUrl: '/aws-sdk-pandas/',

  organizationName: 'aws',
  projectName: 'aws-sdk-pandas',
  trailingSlash: false,

  onBrokenLinks: 'throw',

  markdown: {
    // .md files (including generated tutorial pages) are CommonMark, .mdx are MDX.
    // Generated notebook output contains raw HTML/braces that MDX would reject.
    format: 'detect',
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  themes: [
    '@docusaurus/theme-mermaid',
    [
      require.resolve('@easyops-cn/docusaurus-search-local'),
      {
        hashed: true,
        indexBlog: false,
        docsRouteBasePath: '/',
        highlightSearchTermsOnTargetPage: false,
      },
    ],
  ],

  presets: [
    [
      'classic',
      {
        docs: {
          routeBasePath: '/', // docs at the site root, like hermes-agent — no landing page
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/aws/aws-sdk-pandas/edit/main/website/',
          // ReadTheDocs-style versioning: the released snapshot is the
          // default at the site root ("stable"); main lives at /latest.
          // Cut a new snapshot on release with:
          //   npm run docusaurus -- docs:version <x.y.z>
          // then remove the previous entry from versions.json (we keep one
          // stable snapshot; older versions remain on ReadTheDocs).
          lastVersion: '3.17.1',
          versions: {
            current: {
              label: 'latest (main)',
              path: 'latest',
              banner: 'unreleased',
            },
            '3.17.1': {
              label: '3.17.1 (stable)',
            },
          },
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/logo.png',
    colorMode: {
      defaultMode: 'light',
      respectPrefersColorScheme: true,
    },
    docs: {
      sidebar: {
        hideable: true,
        autoCollapseCategories: true,
      },
    },
    navbar: {
      title: 'AWS SDK for pandas',
      logo: {
        alt: 'AWS SDK for pandas',
        src: 'img/logo.png',
      },
      items: [
        {
          to: '/tutorials',
          label: 'Tutorials',
          position: 'left',
        },
        {
          to: '/api',
          label: 'API Reference',
          position: 'left',
        },
        {
          type: 'docsVersionDropdown',
          position: 'right',
          dropdownActiveClassDisabled: true,
        },
        {
          href: 'https://github.com/aws/aws-sdk-pandas',
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
            { label: 'Install', to: '/install' },
            { label: 'At Scale', to: '/scale' },
            { label: 'Tutorials', to: '/tutorials' },
            { label: 'API Reference', to: '/api' },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'GitHub Issues',
              href: 'https://github.com/aws/aws-sdk-pandas/issues',
            },
            {
              label: 'Discussions',
              href: 'https://github.com/aws/aws-sdk-pandas/discussions',
            },
            {
              label: 'Community Resources',
              href: 'https://github.com/aws/aws-sdk-pandas#community-resources',
            },
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/aws/aws-sdk-pandas',
            },
            {
              label: 'Contributing',
              href: 'https://github.com/aws/aws-sdk-pandas/blob/main/CONTRIBUTING.md',
            },
            {
              label: 'License',
              href: 'https://github.com/aws/aws-sdk-pandas/blob/main/LICENSE.txt',
            },
          ],
        },
      ],
      copyright: `An AWS Professional Services open source initiative | aws-proserve-opensource@amazon.com<br/>Copyright © ${new Date().getFullYear()} Amazon.com, Inc. or its affiliates. All Rights Reserved. Apache-2.0 License.`,
    },
    prism: {
      // Both modes render code on a dark panel (ink on paper in light mode),
      // so both need a dark token theme — a light one would be unreadable.
      theme: prismThemes.oneDark,
      darkTheme: prismThemes.oneDark,
      additionalLanguages: ['bash', 'json', 'python'],
    },
    mermaid: {
      theme: { light: 'neutral', dark: 'dark' },
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
