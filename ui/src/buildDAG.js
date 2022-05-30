import dagreD3 from "dagre-d3";
import * as d3 from "d3";

export const buildDAG = (taskFilterDispatcher, graphData) => {


    const buildGraph = () => {

        const g = new dagreD3.graphlib.Graph().setGraph({})

        graphData.taskGroups.forEach(task => {

             g.setNode(
                 task.key_group,
                 {
                     shape: "task",
                     props: {key_group: task.key_group}
                 }
             )
        })

        graphData.deps.forEach(dep => {

            const [consuming_task, link, producing_task] = dep

            g.setEdge(producing_task, consuming_task, {})
        })

        return g
    }

    const svg = d3.select("svg")
    const inner = svg.select("g")

    const zoom = d3.zoom().on("zoom", (event) => {

        inner.attr("transform", event.transform)
    })

    svg.call(zoom);

    // Create the renderer
    const render = new dagreD3.render();


    render.shapes().task = (parent, bbox, node) => {

        const w = bbox.width * 2;
        const h = bbox.height * 2;

        const shapeSvg = parent
            .insert('rect', ':first-child')
            .attr('rx', 8)
            .attr('width', w)
            .attr('height', h)
            .attr('transform', `translate(${-w/2},${-h/2})`)
            .on("click", () => taskFilterDispatcher({
                   name: 'selectTaskGroup',
                   displayState: null,
                   keyPrefix: node.props.key_group
            }))

        node.intersect = function(point) {
             return dagreD3.intersect.rect(node, point);
        }

        parent.select("text")
           .attr("font-size", "14")
           .attr('transform', `translate(${0},${- h/2 + 12})`)

        const taskCount = (displayState, count, xDelta, color, yDelta) =>
            parent
               .append("text")
               .text(`${count}`)
               .attr("class", `task_count_${displayState}_${node.props.key_group}`)
               .attr("font-size", "12")
               .attr('transform', `translate(${xDelta},${yDelta})`)
               .attr("stroke", color)
               .attr("cursor", "pointer")
               .attr("opacity", 0.5)
               .on("click", () => taskFilterDispatcher({
                   name: 'selectTaskGroup',
                   displayState,
                   keyPrefix: node.props.key_group
               }))

        const firstLine = 5
        taskCount("waiting", "", -50, "grey", firstLine)
        taskCount("running", "", -10, "green", firstLine)
        taskCount("completed", "", 30, "black", firstLine)

        const secondLine = 24
        taskCount("failed", "", -30, "red", secondLine)
        taskCount("ignored", "", 10, "grey", secondLine)


        return shapeSvg
    }

    const g = buildGraph()
    render(inner, g)

    // Center the graph
    const initialScale = 1.75

    svg.call(
        zoom.transform,
        d3.zoomIdentity.translate(
            (svg.attr("width") - g.graph().width * initialScale) / 2, 20
        ).scale(initialScale)
    )

    svg.attr('height', g.graph().height * initialScale + 80)
}
