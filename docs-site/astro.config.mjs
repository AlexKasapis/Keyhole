// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
	site: 'https://docs.keyholetui.com',
	integrations: [
		starlight({
			title: 'Keyhole',
			description:
				'Documentation for Keyhole — a terminal UI for Redis, AMQP 1.0 and RabbitMQ.',
			logo: { src: './src/assets/keyhole.svg', alt: 'Keyhole' },
			favicon: '/favicon.svg',
			social: [
				{
					icon: 'github',
					label: 'GitHub',
					href: 'https://github.com/AlexKasapis/Keyhole',
				},
			],
			editLink: {
				baseUrl:
					'https://github.com/AlexKasapis/Keyhole/edit/main/docs-site/',
			},
			sidebar: [
				{
					label: 'Start here',
					items: [
						{ label: 'Introduction', link: '/' },
						{ label: 'Installation', slug: 'guides/installation' },
						{ label: 'Quick start', slug: 'guides/quick-start' },
					],
				},
				{
					label: 'Using Keyhole',
					items: [
						{ label: 'Supported brokers', slug: 'guides/brokers' },
						{ label: 'Recording streams', slug: 'guides/recording' },
					],
				},
			],
		}),
	],
});
