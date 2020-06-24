import React from 'react';
import ReactDOM from 'react-dom';
import * as Sentry from '@sentry/browser';
import './index.css';
import App from './App';
import {BrowserRouter} from "react-router-dom";

Sentry.init({dsn: process.env.SENTRY_DSN});

ReactDOM.render(<BrowserRouter><App /></BrowserRouter>, document.getElementById('root'));
