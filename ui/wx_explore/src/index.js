import React from 'react';
import ReactDOM from 'react-dom';
import * as Sentry from '@sentry/browser';
import './index.css';
import App from './App';

Sentry.init({dsn: process.env.SENTRY_DSN});

ReactDOM.render(<App />, document.getElementById('root'));
