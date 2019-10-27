import React from 'react';
import {AsyncTypeahead} from 'react-bootstrap-typeahead';

import Api from './Api';

const LocationResult = ({loc}) => (
  <div>
    <span>{loc.name}</span>
  </div>
);

export default class LocationSearchField extends React.Component {
  state = {
    isLoading: false,
    options: [],
  };

  render() {
    return (
      <AsyncTypeahead
        {...this.state}
        placeholder="Location"
        minLength={3}
        onSearch={this._handleSearch}
        labelKey="name"
        renderMenuItemChildren={(option, props) => (
          <LocationResult key={option.id} loc={option} />
        )}
        onChange={this.props.onChange}
      />
    );
  }

  _handleSearch = (query) => {
    this.setState({isLoading: true});
    Api.get("/location/search", {
      params: {
        q: query,
      }
    }).then(({data}) => {
      this.setState({
        isLoading: false,
        options: data,
      });
    });
  }
}
