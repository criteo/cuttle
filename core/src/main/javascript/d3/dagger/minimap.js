import cytoscape from 'cytoscape';
import cytoscapedagre from 'cytoscape-dagre';
import dagre from 'dagre';
import _ from 'lodash';
import * as d3 from 'd3';

import CSSescape from 'css.escape';

cytoscapedagre(cytoscape, dagre);

export const draw = (graphData, minimapContainer) => {
  const minimap = cytoscape({
		//container: minimapContainer,
		layout: {
            name: 'dagre',
    				nodeSep: 3,
            rankSep: 300,
            rankDir: 'LR'
          },
					style: [
						{
							selector: 'node',
							style: {
								'background-color': '#90A4AE',
                shape: "rectangle",
                rx: 2.5,
                ry: 2.5,
                padding: 0,
                width: 120,
                height: 60
							}
						},
						{
							selector: 'edge',
							style: {
								'width': 0.8,
								'curve-style': 'segments',
								'line-color': '#777',
							}
						}
					],

					elements: {
						nodes: graphData.nodes.map(n => ({data: {...n}})),
						edges: graphData.edges.map(n => ({data: {...n, id: n.source + n.target}}))
					}
				});
        minimap.userZoomingEnabled(false);
				minimap.userPanningEnabled(false);
				minimap.nodes().lock();

        return minimap;
};

export const getNodesOrder = minimap => {
	const renderedNodes = minimap.nodes().map(n => ({id: n.id(), ...n.renderedPosition()}));
	return _.orderBy(renderedNodes, ['x', 'y']).map((n, i) => ({id: n.id, order: i, yPosition: n.y}));
}

const activeNodeStyle = {
	'background-color': "#42A5F5",
	'shadow-blur': 80,
	'shadow-opacity': 1,
	'shadow-color': "#42A5F5"
};
const activeLineStyle = {
	'line-color': "#42A5F5",
	width: 3
}
const inactiveNodeStyle = {
	'background-color': '#90A4AE',
	'shadow-blur': 0,
	'shadow-opacity': 0,
	'shadow-color': "#42A5F5"

}
const inactiveLineStyle = {
	'line-color': '#777',
	width: 0.8
}

export const enter = (nodesSelection, edgesSelection, minimap) => {
	const nodesPromise = new Promise(resolve => {
		nodesSelection
			.data()
			.forEach(d =>
					minimap.getElementById(d.id)
						.style(activeNodeStyle));
		resolve("minimap enter nodes done");
	});
	const edgesPromise = new Promise(resolve => {
		edgesSelection
			.data()
			.forEach(d =>
					minimap.getElementById(d.id).style(activeLineStyle));
		resolve("minimap enter edges done");
	});
	return [nodesPromise, edgesPromise];
};

export const update = (nodesSelection, edgesSelection, minimap) => {
	const nodesPromise = new Promise(resolve => {
	  nodesSelection
			.data()
			.forEach(d =>
					minimap.getElementById(d.id)
						.style(activeNodeStyle));
		resolve("minimap update nodes done");
	});
	const edgesPromise = new Promise(resolve => {
		edgesSelection
			.data()
			.forEach(d =>
					minimap.getElementById(d.id).style(activeLineStyle));
		resolve("minimap update edges done");
	});
	return [nodesPromise, edgesPromise];
};

export const exit = (nodesSelection, edgesSelection, minimap) => {
	const nodesPromise = new Promise(resolve => {
		nodesSelection
				.data()
				.forEach(d =>
						minimap.getElementById(d.id)
							.style(inactiveNodeStyle));
		resolve("minimap exit nodes done");
	});
	const edgesPromise = new Promise(resolve => {
		edgesSelection
			.data()
			.forEach(d =>
					minimap.getElementById(d.id)
						.style(inactiveLineStyle));
		resolve("minimap exit nodes done");
	});
	return [nodesPromise, edgesPromise];
};
