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
    if (prevProps.location != null && this.props.location.id === prevProps.location.id) {
      return;
    }

    if (this.props.location === undefined || this.props.location == null) {
      return;
    }
    
    this.getWx();
  }

  chartjsData() {
    let labels = [];
    let metrics = {};

    for (const ts of this.state.wx.ordered_times) {
      labels.push(moment.unix(ts).format("h:mA dddd Do")); // 8:15PM Tuesday 15th
      for (const data_point of this.state.wx.data[ts]) {
        const metric_id = this.state.source_fields[data_point.src_field_id].metric_id;
        if (!(metric_id in metrics)) {
          metrics[metric_id] = {
            label: this.state.metrics[metric_id].name,
            data: []
          };
        }
        metrics[metric_id].data.push(data_point.value);
      }
    }

    let datasets = [];
    for (const mid in metrics) {
      datasets.push(metrics[mid]);
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

    return (
      <div>
      <span>{this.props.location.name}</span>
      <LineChart data={this.chartjsData()} />
      </div>
    );
  }
}
