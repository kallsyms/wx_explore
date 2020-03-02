import React from 'react';
import Nav from 'react-bootstrap/Nav';
import Navbar from 'react-bootstrap/Navbar';
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Form from 'react-bootstrap/Form';

import { FontAwesomeIcon } from '@fortawesome/react-fontawesome'
import { faGithub } from '@fortawesome/free-brands-svg-icons'

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
    const year = new Date().getFullYear();

    let body;

    if (this.state.location == null) {
      body = (
        <Row className="justify-content-md-center">
          <Col md="auto">
            <h4>Enter you location to get started</h4>
          </Col>
        </Row>
      );
    } else {
      body = (
        <ForecastView location={this.state.location} converter={this.state.unitConverter}/>
      );
    }

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
          {body}
        </Container>
        <footer class="footer">
          <Container fluid={true} style={{height: "100%"}}>
            <Row className="h-100">
              <Col xs="4" className="align-self-center">
                <a target="_blank" rel="noopener noreferrer" href="https://github.com/kallsyms/wx_explore" class="text-muted">
                  kallsyms/wx_explore on <FontAwesomeIcon icon={faGithub} size="lg"/>
                </a>
                <br/>
                <span class="text-muted">&copy; {year}</span>
              </Col>
              <Col xs="8" className="align-self-center">
                <span class="text-muted" style={{fontSize: "0.75em"}}>
                  The data on this website is best-effort, and no guarantees are made about the availability or correctness of the data. It should not be used for critical decision making.
                </span>
              </Col>
            </Row>
          </Container>
        </footer>
      </div>
    );
  }
}
