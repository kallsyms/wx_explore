import React from 'react';
import Navbar from 'react-bootstrap/Navbar';
import Form from 'react-bootstrap/Form';

import LocationSearchField from './LocationSearch';
import ForecastView from './ForecastView';

import './App.css';

export default class App extends React.Component {
  state = {
    location: null,
  };

  setLocation(loc) {
    // onChange passes loc=null when the old selected entry is cleared
    if (loc == null) {
      return;
    }

    this.setState({
      location: loc,
    });
  }

  componentDidMount() {
    if (!navigator.geolocation) {
      return;
    }

    navigator.geolocation.getCurrentPosition((position) => {
      const lat = position.coords.latitude;
      const lon = position.coords.longitude;

      this.setLocation({lat, lon});
    });
  }

  render() {
    return (
      <div className="App">
        <Navbar className="bg-dark justify-content-between">
          <Form inline>
          </Form>
          <Form inline>
            <LocationSearchField onChange={(selected) => {this.setLocation(selected[0])}}/>
          </Form>
        </Navbar>
        <ForecastView location={this.state.location} />
      </div>
    );
  }
}
