package concurrency

import "fmt"

type Stat int

const (
	Unvisited Stat = iota
	Visiting
	Done
)

type Stack struct {
	elements []int
}

func NewStack(capacity int) *Stack {
	return &Stack{
		elements: make([]int, 0, capacity),
	}
}

func (s *Stack) Push(i int) {
	s.elements = append(s.elements, i)
}

func (s *Stack) Pop() {
	s.elements = s.elements[0 : len(s.elements)-1]
}

func (s *Stack) Top() int {
	if len(s.elements) == 0 {
		panic("empty stack")
	}
	return s.elements[len(s.elements)-1]
}

type context struct {
	stats     []Stat
	stack     *Stack
	hasCircle bool
	circle    []int
}

func newContext(stats []Stat, capacity int) *context {
	return &context{
		stats: stats,
		stack: NewStack(capacity),
	}
}

type Visitor func(*context, int) (kontinue bool)

type Graph struct {
	matrix [][]int
}

func NewGraph(graph [][]int) *Graph {
	return &Graph{matrix: graph}
}

func (g *Graph) newContext() *context {
	return newContext(make([]Stat, len(g.matrix)), len(g.matrix)*3)
}

func (g *Graph) getNeighbors(node int) (neighbors []int) {
	for j, v := range g.matrix[node] {
		if v == 1 {
			neighbors = append(neighbors, j)
		}
	}
	return
}

func (g *Graph) SearchCircle() []int {
	ctx := g.newContext()
	g.dfs(ctx, 1, func(c *context, i int) (kontinue bool) {
		fmt.Printf("%v\n", ctx.stack.elements)
		return true
	})
	return ctx.circle
}

func (g *Graph) dfs(ctx *context, node int, visitor Visitor) (kontinue bool) {
	switch ctx.stats[node] {
	case Done:
		fmt.Printf("visited %d, %v\n", node, ctx.stack.elements)
		return true
	case Visiting:
		ctx.hasCircle = true
		for i, ele := range ctx.stack.elements {
			if ele == node {
				for j := i; j < len(ctx.stack.elements); j++ {
					ctx.circle = append(ctx.circle, ctx.stack.elements[j])
				}
				ctx.circle = append(ctx.circle, node)
				break
			}
		}
		if len(ctx.circle) == 0 {
			panic("circle elements empty")
		}
		return false
	default:
		ctx.stats[node] = Visiting
		ctx.stack.Push(node)
		if !visitor(ctx, node) {
			return false
		}
	}

	neighbors := g.getNeighbors(node)
	for _, neighbor := range neighbors {
		if !g.dfs(ctx, neighbor, visitor) {
			return false
		}
	}

	ctx.stats[node] = Done
	ctx.stack.Pop()

	return true
}
