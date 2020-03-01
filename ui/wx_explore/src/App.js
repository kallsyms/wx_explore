import React from 'react';
import Nav from 'react-bootstrap/Nav';
import Navbar from 'react-bootstrap/Navbar';
import Container from 'react-bootstrap/Container';
import Form from 'react-bootstrap/Form';

import Api from './Api';
import ForecastView from './ForecastView';
import LocationSearchField from './LocationSearch';
import {Imperial, Metric} from './Units'

import './App.css';

export default class App extends React.Component {
  state = {
    location: null,
    unitConverter: new Imperial(),
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

      Api.get("/location/by_coords", {
        params: {lat, lon},
      }).then(({data}) => this.setLocation({lat, lon, name: data.name}));
    });
  }

  render() {
    return (
      <div className="App">
        <Navbar bg="dark" variant="dark">
          <Navbar.Brand>Wx Explore</Navbar.Brand>
          <Nav className="mr-auto"></Nav>

          <Form inline>
            <LocationSearchField onChange={(selected) => {this.setLocation(selected[0])}}/>
          </Form>
        </Navbar>
        <Container style={{marginTop: "1em"}}>
          <ForecastView location={this.state.location} converter={this.state.unitConverter}/>
        </Container>
      </div>
    );
  }
}
