import React from 'react';
import moment from 'moment';
import {Line as LineChart} from 'react-chartjs-2';

import Api from './Api';

export default class ForecastView extends React.Component {
  state = {
    wx: null,
    sources: null,
    source_fields: null,
    metrics: null,
  };

  getWx() {
    Api.get("/location/" + this.props.location.id + "/wx").then(({data}) => this.setState({wx: data}));
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

    if (this.props.location === undefined || this.props.location == null) {
      return;
    }
    
    this.getWx();
  }

  componentDidUpdate(prevProps) {
    // only attempt to fetch when we have a location...
    if (this.props.location === undefined || this.props.location == null) {
      return;
    }
    
    // ... or when location changed
    if (prevProps.location != null && this.props.location.id === prevProps.location.id) {
      return;
    }

    this.getWx();
  }

  chartjsData() {
    let labels = [];
    let metrics = {}; // map[metric_id, map[source_id, map[run_time, list]]] 

    for (const ts of this.state.wx.ordered_times) {
      labels.push(moment.unix(ts).format("h:mmA dddd Do")); // 8:15PM Tuesday 15th
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

        metrics[metric.id][source.id][data_point.run_time].push(data_point.value);
      }
    }

    let datasets = {};
    for (const metric_id in metrics) {
      datasets[metric_id] = [];

      for (const source_id in metrics[metric_id]) {
        for (const run_time in metrics[metric_id][source_id]) {
          const metric = this.state.metrics[metric_id];
          const source = this.state.sources[source_id];
          const run_name = moment.unix(run_time).format("h:mmA dddd Do") + " " + source.name;
          datasets[metric_id].push({
            label: run_name,
            data: metrics[metric_id][source_id][run_time],
            fill: false,
          });
        }
      }
    }

    return {
      labels,
      datasets,
    };
  }

  render() {
    if (this.state.wx == null || this.state.sources == null || this.state.source_fields == null || this.state.metrics == null) {
      return null;
    }

    let {labels, datasets} = this.chartjsData();
    let charts = [];

    for (const metric_id in datasets) {
      const data = {
        labels: labels,
        datasets: datasets[metric_id],
      };
      charts.push(<LineChart data={data} />);
    };

    return (
      <div>
      <span>{this.props.location.name}</span>
      {charts}
      </div>
    );
  }
}
