// SPDX-License-Identifier: MIT
// Copyright contributors to the kepler.gl project

import React, {Dispatch, useEffect, useState} from 'react';
import ReactDOM from 'react-dom/client';
import {connect, Provider} from 'react-redux';

import {applyMiddleware, combineReducers, compose, createStore} from 'redux';

import KeplerGl from '@kepler.gl/components';
import keplerGlReducer, {enhanceReduxMiddleware, KeplerGlState} from '@kepler.gl/reducers';

import AutoSizer from 'react-virtualized/dist/commonjs/AutoSizer';
import rawData from './output.json'
import { processRowObject} from '@kepler.gl/processors';
import {
  addDataToMap,
  updateMap,
  replaceDataInMap
} from '@kepler.gl/actions';
// create reducers
const reducers = combineReducers({
  // mount keplerGl reducer
  keplerGl: keplerGlReducer.initialState({
    uiState: {
      readOnly: false,
      currentModal: null
    }
  })
});

// create middlewares
const middleWares = enhanceReduxMiddleware([
  // Add other middlewares here
]);

// craeteEnhancers
const enhancers = applyMiddleware(...middleWares);

// create store
const initialState = {};
const store = createStore(reducers, initialState, compose(enhancers));

export const configgeo = {
  version: 'v1',
  config: {
    visState: {
      filters: [],
      layers: [

         {
          id: 'iulge5f',
          type: 'point',
          config: {
            dataId: 'sample_visit_data',
            label: 'point',
             columns: {
              lat: 'latitude',
              lng: 'longitude'
            },
            isVisible: true,
            color:[255, 0, 0],
            visConfig: {
                       radius:3,
            }

          }
        },
        {
          id: 'iulge5',
          type: 'geojson',
          color:[0, 0, 255],
          config: {
            dataId: 'sample_visit_data',
            label: 'gejosn',
             columns: {
              geojson: 'polygon'
            },
            isVisible: true
          }
        },

        
      ]
    }
  }
};
const App = () => {
   const [rowcount,setRowCount] = useState(0)

  const addDataToKep = () => {
   // store.dispatch(
       if(rowcount===0){
     store.dispatch(
       addDataToMap({
        datasets: [
          {
            info: {
              label: 'Sample Visit Data' ,
              id: 'sample_visit_data'
            },
            data: processRowObject([rawData[rowcount]])
          }
        ],
     config: configgeo,
       options:{
        autoCreateLayers: true,
        autoCreateTooltips:true
       }
      })
     )

    } else {
      store.dispatch(replaceDataInMap({
        datasetToReplaceId : 'sample_visit_data',
        datasetToUse: {
           
             info:{
              label: 'Sample Visit Data' ,
              id: 'sample_visit_data',
             },

              data: processRowObject([rawData[rowcount]])
            },
            
        
      }))
    }
   // );
    setTimeout(() =>{
      store.dispatch(
        updateMap({
          zoom: 18.5
        })
      )
    },500)
  }

  useEffect(()=>{
addDataToKep()
addDataToKep()

  },[rowcount])
  return (
    <div
    style={{
      position: 'absolute',
      top: '0px',
      left: '0px',
      width: '100%',
      height: '100%'
    }}
  >

    <button style={{
       position: 'absolute',
      top: '0px', 
      right: '60px',
      zIndex:99999
    }}
    onClick={() =>{
      setRowCount(prev => Math.min(prev+1,rawData.length-1))
    }}
    >
      NEXT POLYGON
    </button>


    <AutoSizer>
      {({height, width}) => (
        <KeplerGl
          mapboxApiAccessToken="<MAPBOX_ACCESS_TOKEN>"
          id="map"
          width={width}
          height={height}
        />
      )}
    </AutoSizer>
  </div>
  )
};

const mapStateToProps = (state: KeplerGlState) => state;
const dispatchToProps = (dispatch: Dispatch<any>) => ({dispatch});
const ConnectedApp = connect(mapStateToProps, dispatchToProps)(App);
const Root = () => (
  <Provider store={store}>
    <ConnectedApp />
  </Provider>
);

const container = document.getElementById('root');
if (container) {
  const root = ReactDOM.createRoot(container);
  root.render(<Root />);
}
