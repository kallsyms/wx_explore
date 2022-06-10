import React from 'react';
import moment from 'moment';
import {Line as LineChart} from 'react-chartjs-2';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Spinner from 'react-bootstrap/Spinner';

import Api from './Api';

const lineColors = {
  'hrrr': '255,0,0',
  'gfs':  '0,255,0',
  'nam':  '0,0,255',
};

const metricsToDisplay = [
  "1", // temperature
  "3", // rain
  "6", // snow
  "12", // wind
  "15", // cloud cover
];

function capitalize(s) {
  return s[0].toUpperCase() + s.substring(1)
}

export default class ForecastView extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      location: null,
      metrics: null,
      sources: null,
      source_fields: null,
      summary: null,
      wx: null,
    };
  }

  getWx() {
    let t = Math.round((new Date()).getTime() / 1000);

    Api.get("/wx", {
      params: {
        lat: this.state.location.lat,
        lon: this.state.location.lon,
        start: t,
        end: t + (3 * 24 * 60 * 60), // 3 days out
      },
    }).then(({data}) => this.setState({wx: data}));

    Api.get("/wx/summarize", {
      params: {
        lat: this.state.location.lat,
        lon: this.state.location.lon,
        days: 1,
      },
    }).then(({data}) => this.setState({summary: data}));
  }

  componentDidMount() {
    Api.get("/sources").then(({data}) => {
        let sources = {}
        let source_fields = {};
        for (let src of data) {
            sources[src.id] = src;
            for (let field of src.fields) {
                source_fields[field.id] = field;
            }
        }
        this.setState({sources, source_fields});
    });

    Api.get("/metrics").then(({data}) => {
        let metrics = {}
        for (let metric of data) {
            metrics[metric.id] = metric;
        }
        this.setState({metrics});
    });
    
    if (this.props.match.params.loc_id !== undefined) {
      Api.get(`/location/${this.props.match.params.loc_id}`).then(({data}) => {
        this.setState({location: data});
      });
    } else if (this.props.match.params.lat !== undefined && this.props.match.params.lon !== undefined) {
      Api.get("/location/by_coords", {
        params: {
          lat: this.props.match.params.lat,
          lon: this.props.match.params.lon,
        },
      }).then(({data}) => {
        this.setState({
          location: {
            lat: this.props.match.params.lat,
            lon: this.props.match.params.lon,
            name: `Near ${data.name}`,
          }
        });
      });
    }
  }

  componentDidUpdate(prevProps, prevState) {
    // only attempt to fetch when we have a location...
    if (this.state.location == null) {
      return;
    }
    
    // ... or when location changed
    if (prevState.location === this.state.location) {
      return;
    }

    this.setState({wx: null, summary: null});
    this.getWx();
  }

  chartjsData() {
    let metrics = {}; // map[metric_id, map[source_id, map[run_time, list]]] 

    for (const ts of this.state.wx.ordered_times) {
      for (const data_point of this.state.wx.data[ts]) {
        const source_field = this.state.source_fields[data_point.src_field_id]
        const metric = this.state.metrics[source_field.metric_id];
        const source = this.state.sources[source_field.source_id];

        if (!(metric.id in metrics)) {
          metrics[metric.id] = {};
        }

        if (!(source.id in metrics[metric.id])) {
          metrics[metric.id][source.id] = {};
        }

        if (!(data_point.run_time in metrics[metric.id][source.id])) {
          metrics[metric.id][source.id][data_point.run_time] = [];
        }

        const [val, ] = this.props.converter.convert(data_point.value, metric.units);
        metrics[metric.id][source.id][data_point.run_time].push({x: new Date(ts * 1000), y: val});
      }
    }

    let datasets = {};
    for (const metric_id in metrics) {
      if (!metricsToDisplay.includes(metric_id)) {
        continue;
      }

      datasets[metric_id] = [];

      for (const source_id in metrics[metric_id]) {
        const source = this.state.sources[source_id];

        let earliest_run = 0;
        let latest_run = 0;
        for (const run_time in metrics[metric_id][source_id]) {
          if (earliest_run === 0 || run_time < earliest_run) {
            earliest_run = run_time;
          } else if (run_time > latest_run) {
            latest_run = run_time;
          }
        }

        for (const run_time in metrics[metric_id][source_id]) {
          let alpha = 0.15;
          if (run_time === latest_run) {
            alpha = 0.8;
          }

          const run_name = moment.unix(run_time).utc().format("HH[Z] dddd Do") + " " + source.name;
          const color = 'rgba('+lineColors[source.short_name]+','+alpha+')';

          datasets[metric_id].push({
            label: run_name,
            data: metrics[metric_id][source_id][run_time],
            fill: false,
            backgroundColor: color,
            borderColor: color,
            pointBorderColor: color,
          });
        }
      }
    }

    return datasets;
  }

  coreMetricsBox(day) {
    const summary = this.state.summary[day];

    let cloudCoverIcon = '';
    switch (summary.cloud_cover[0].cover) {
      case 'clear':
        cloudCoverIcon = 'wi-day-sunny';
        break;
      case 'mostly clear':
        cloudCoverIcon = 'wi-day-sunny';
        break;
      case 'partly cloudy':
        cloudCoverIcon = 'wi-day-cloudy-high';
        break;
      case 'mostly cloudy':
        cloudCoverIcon = 'wi-cloud';
        break;
      case 'cloudy':
        cloudCoverIcon = 'wi-cloudy';
        break;
      default:
        cloudCoverIcon = 'wi-alien'; // idk
    }

    return (
      <Row className="justify-content-md-center">
        <Col md={2}>
          <i style={{fontSize: "7em"}} className={"wi " + cloudCoverIcon}></i>
        </Col>
        <Col md={3}>
          <h4>{this.props.converter.convert(summary.temps[0].temperature, 'K')} {capitalize(summary.cloud_cover[0].cover)}</h4>
          <p>High: {this.props.converter.convert(summary.high.temperature, 'K')}</p>
          <p>Low: {this.props.converter.convert(summary.low.temperature, 'K')}</p>
        </Col>
      </Row>
    );
  }

  summarize(day) {
    let components = [];

    for (const [index, component] of this.state.summary[day].summary.components.entries()) {
      let text = '';
      if (index === 0) {
        text = capitalize(component.text);
      } else {
        text = component.text;
      }
      text += ' ';

      if (component.type === 'text') {
        components.push(<span key={index}>{text}</span>);
      } else {
        components.push(<span key={index}>{text}</span>);
      }
    }

    return (
      <span>{components}</span>
    );
  }

  render() {
    if (this.state.summary == null || this.state.sources == null || this.state.source_fields == null || this.state.metrics == null) {
      return (
        <Spinner animation="border" role="status">
          <span className="sr-only">Loading...</span>
        </Spinner>
      );
    }

    let charts = [];

    if (this.state.wx == null) {
      charts.push(
        <Spinner animation="border" role="status">
          <span className="sr-only">Loading...</span>
        </Spinner>
      );
    } else {
      let datasets = this.chartjsData();
      const options = {
        scales: {
          xAxes: [{
            type: 'time',
            distribution: 'linear',
            time: {
              unit: 'hour',
            },
            ticks: {
              min: moment(),
              max: moment().add(3, 'days'),
            },
          }],
        },
        legend: {
          display: false,
        },
      };

      for (const metric_id in datasets) {
        const metric = this.state.metrics[metric_id];
        const data = {
          datasets: datasets[metric_id],
        };
        let opts = {
          ...options,
          title: {
            display: true,
            text: metric.name,
          },
        };
        charts.push(
          <Row>
            <Col>
              <LineChart key={metric.name} data={data} options={opts}/>
            </Col>
          </Row>
        );
      };
    }

    return (
      <div>
        <Row className="justify-content-md-center">
          <Col md="auto">
            <h2>{this.state.location.name}</h2>
          </Col>
        </Row>
        {this.coreMetricsBox(0)}
        <Row className="justify-content-md-center">
          <Col md="auto">
            <p style={{fontSize: "1.5em"}}>{this.summarize(0)}</p>
          </Col>
        </Row>
        <hr/>
        {charts}
      </div>
    );
  }
}
