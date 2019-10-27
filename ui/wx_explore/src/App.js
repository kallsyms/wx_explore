import React from 'react';
import Navbar from 'react-bootstrap/Navbar';
import Form from 'react-bootstrap/Form';

import Api from './Api';
import LocationSearchField from './LocationSearch';
import ForecastView from './ForecastView';

import './App.css';

export default class App extends React.Component {
  state = {
    location: null,
  };

  setLocation(loc) {
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

      Api.get("/location/by_coords", {params: {lat, lon}}).then(({data}) => {this.setLocation(data)})
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
