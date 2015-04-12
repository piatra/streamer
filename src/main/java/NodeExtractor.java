import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphEdge;
import edu.stanford.nlp.util.MapFactory;
import java.util.*;

public class NodeExtractor {

    private static final MapFactory<IndexedWord, IndexedWord> wordMapFactory = MapFactory.hashMapFactory();
    private SemanticGraph graph;

    public NodeExtractor(SemanticGraph graph1) {
        graph = graph1;
    }

    private Collection<IndexedWord> getRoots() {
        return this.graph.getRoots();
    }

    public ArrayList<ArrayList<String>> getAll() {
        Collection rootNodes = this.getRoots();
        ArrayList<ArrayList<String>> result = new ArrayList<>();
        if(!rootNodes.isEmpty()) {
            Set used = wordMapFactory.newSet();
            Iterator nodes = rootNodes.iterator();

            IndexedWord node;
            while (nodes.hasNext()) {
                node = (IndexedWord) nodes.next();
                ArrayList<String> tuple = new ArrayList<>();

                tuple.add(node.tag());
                tuple.add(node.ner());
                tuple.add(node.originalText());
                tuple.add(String.valueOf(1));
                result.add(tuple);

                this.recToString(node, result, 1, used);
            }

            Set nodes1 = wordMapFactory.newSet();
            nodes1.addAll(this.graph.vertexSet());
            nodes1.removeAll(used);

            while (!nodes1.isEmpty()) {
                node = (IndexedWord) nodes1.iterator().next();
                ArrayList<String> tuple = new ArrayList<>();

                tuple.add(node.tag());
                tuple.add(node.ner());
                tuple.add(node.originalText());
                tuple.add(String.valueOf(1));
                result.add(tuple);

                this.recToString(node, result, 1, used);
                nodes1.removeAll(used);
            }
        }

        return result;
    }

    private void recToString(IndexedWord curr, ArrayList<ArrayList<String>> result, int offset, Set<IndexedWord> used) {
        used.add(curr);
        List edges = this.graph.outgoingEdgeList(curr);
        Collections.sort(edges);
        Iterator var7 = edges.iterator();

        while (var7.hasNext()) {
            SemanticGraphEdge edge = (SemanticGraphEdge)var7.next();
            IndexedWord target = edge.getTarget();
            ArrayList<String> tuple = new ArrayList<>();

            tuple.add(target.tag());
            tuple.add(target.ner());
            tuple.add(target.originalText());
            tuple.add(String.valueOf(offset));
            result.add(tuple);

            if(!used.contains(target)) {
                this.recToString(target, result, offset + 1, used);
            }
        }

    }
}
