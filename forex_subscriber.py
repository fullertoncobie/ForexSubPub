import socket
from datetime import datetime, timedelta
import fxp_bytes_subscriber
import bellman_ford
import math
import threading
import queue
import time

class Subscriber:
    """
    A subscriber client that connects to a ForexProvider publisher service.
    Monitors prices for arbitrage opportunities using the  Bellman-Ford algorithm.
    
    Attributes:
        server_address: Tuple of (host, port) for the ForexProvider server
        socket: UDP socket for receiving price updates
        quotes_dict: Dictionary storing current forex prices and timestamps
        current_path: Currently identified arbitrage path, if any
        quotes_graph: Graph representation of forex pairs for arbitrage detection
        dict_lock: Thread lock for synchronizing access to shared data
        quotes_queue: Queue for processing incoming quote updates
        running: Boolean flag to control thread execution
    """
    def __init__(self, server_address):
        """
        Initialize a new Subscriber instance.
        
        Args:
            server_address: Tuple of (host, port) for the ForexProvider server
        """
        self.server_address = server_address
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("127.0.0.1", 0))
        self.quotes_dict = {}
        self.current_path = None
        self.quotes_graph = bellman_ford.BellmanFord()
        self.dict_lock = threading.Lock()
        self.quotes_queue = queue.Queue()
        self.running = True
        
    def subscribe(self):
        """
        Send a subscription request to the ForexProvider server.
        """
        try:
            subscription_request = fxp_bytes_subscriber.serialize_address(self.socket.getsockname())
            self.socket.sendto(subscription_request, self.server_address)
            print(f"Subscription request sent to {self.server_address}")
        except Exception as e:
            print(f"Subscription failed: {e}")

    def update_quotes_dict(self, new_quotes_dict):
        """
        Update the internal quotes dictionary with new price information.
        
        Args:
            new_quotes_dict: Dictionary of new forex quotes {currency_pair: (price, timestamp)}
            
        Returns:
            bool: True if any quotes were updated, False otherwise
        """
        updated = False
        with self.dict_lock:
            for iso_pair, (price, timestamp) in new_quotes_dict.items():
                # Add new currency pairs to our tracking
                if iso_pair not in self.quotes_dict:
                    print(f"Adding new quotes for: {iso_pair}")
                    self.quotes_dict[iso_pair] = (price, timestamp)
                    updated = True
                # Update existing pairs only if we have newer data
                elif self.quotes_dict[iso_pair][1] < timestamp:
                    self.quotes_dict[iso_pair] = (price, timestamp)
                    updated = True
                else:
                    print(f"Outdated quote for: {iso_pair} received")
            
            # If we made any updates, notify the graph update thread
            if updated:
                self.quotes_queue.put(dict(self.quotes_dict))

    def remove_stale_quotes(self):
        """
        Remove quotes older than 1.5 seconds from the quotes dictionary.
        Updates the quotes queue if any quotes are removed.
        """
        current_time = datetime.utcnow()
        with self.dict_lock:
            # Collect keys to remove first to avoid modifying dict during iteration
            stale_pairs = []
            
            for iso_pair, (price, timestamp) in self.quotes_dict.items():
                age = (current_time - timestamp).total_seconds()
                if age > 1.5:  # Quotes older than 1.5 seconds are considered stale
                    print(f"Removing quote {iso_pair}: {price} aged {age} seconds")
                    stale_pairs.append(iso_pair)
            
            # Remove the stale quotes
            for iso_pair in stale_pairs:
                del self.quotes_dict[iso_pair]
            
            # Notify graph update thread if quotes were removed
            if stale_pairs:
                self.quotes_queue.put(dict(self.quotes_dict))

    def update_graph(self):
        """
        Continuously update the forex graph with new quotes and check for arbitrage opportunities.
        Runs in a separate thread to process the quotes queue.
        """
        while self.running:
            try:
                quotes = self.quotes_queue.get(timeout=0.1)
                
                new_graph = bellman_ford.BellmanFord()
                
                for iso_pair, (price, timestamp) in quotes.items():
                    # Double-check quote freshness before using it
                    age = (datetime.utcnow() - timestamp).total_seconds()
                    if age <= 1.5:  
                        # Negative log for base currency -> quote currency
                        # Positive log for quote currency -> base currency
                        log_price = math.log10(price)
                        new_graph.add_edge(iso_pair[0], iso_pair[1], -log_price)
                        new_graph.add_edge(iso_pair[1], iso_pair[0], log_price)
                
                # Update the graph reference atomically
                with self.dict_lock:
                    self.quotes_graph = new_graph
                
                self.identify_arbitrage()
                
            except queue.Empty:
                continue
            except Exception as e:
                print(f"Error in graph update thread: {e}")
                continue

    def find_negative_cycle_path(self, predecessor, neg_edge):
        """
        Reconstruct the complete negative cycle path from the Bellman-Ford results.
        
        Args:
            predecessor: Dictionary of vertex predecessors from shortest_paths
            neg_edge: Tuple (u, v) representing the edge where negative cycle was detected
        
        Returns:
            list: Ordered list of vertices representing the complete arbitrage cycle,
                 or None if no cycle exists
        """
        if neg_edge is None:
            return None
        
        u, v = neg_edge
        # Initialize cycle with the negative edge endpoints
        cycle = [v, u]
        current = predecessor[u]
        
        # Walk backwards through predecessors until we complete the cycle
        # This reconstructs the full negative cycle from the Bellman-Ford results
        while current not in cycle:
            cycle.append(current)
            current = predecessor[current]
            
        # Extract just the cycle portion (remove path leading to cycle)
        cycle_start = cycle.index(current)
        cycle = cycle[cycle_start:]
        # Add starting vertex to end to complete the cycle
        cycle.append(cycle[0])
        
        # Reverse to get correct order (we built it backwards)
        cycle.reverse()
        
        return cycle

    def identify_arbitrage(self):
        """
        Check for arbitrage opportunities using the Bellman-Ford algorithm.
        If found, prints the arbitrage path and calculated profit from a
        hypothetical 100 unit trade.
        """
        # Run Bellman-Ford starting from USD
        shortest_path = self.quotes_graph.shortest_paths('USD', 0.000000001)
        path = self.find_negative_cycle_path(shortest_path[1], shortest_path[2])

        with self.dict_lock:        
            # Only process if we found a new arbitrage path
            if path and path != self.current_path:
                self.current_path = path
                curr_money = 100  # Start with 100 units of base currency
                print("ARBITRAGE OPPORTUNITY:")

                # Walk through the path calculating conversions
                for i in range(len(path) - 1):  
                    curr_pair = (path[i], path[i + 1])

                    # Look up the exchange rate, handling both quote directions
                    if curr_pair in self.quotes_dict:
                        curr_rate = self.quotes_dict[curr_pair][0]
                    elif curr_pair[::-1] in self.quotes_dict: 
                        # If we have the inverse rate, flip it
                        curr_rate = self.quotes_dict[curr_pair[::-1]][0]
                        curr_rate = 1/curr_rate
                    else:
                        print(f"Could not find price for {curr_pair}")
                    
                    # Calculate running profit through the arbitrage cycle
                    curr_money *= curr_rate
                    print(f"\t{curr_pair[0]} -> {curr_pair[1]}: by {curr_rate} for {curr_money}")

    def receive_updates(self):
        """
        Continuously listen for and process quote updates from the ForexProvider.
        Runs in a separate thread to handle incoming UDP packets.
        """
        while self.running:
            try:
                data, _ = self.socket.recvfrom(4096)
                quotes_binary = fxp_bytes_subscriber.split_quotes(data)
                new_quotes_dict = fxp_bytes_subscriber.parse_quotes(quotes_binary)
                self.update_quotes_dict(new_quotes_dict)
                self.remove_stale_quotes()
            except Exception as e:
                print(f"Error receiving updates: {e}")

    def start(self):
        """
        Start the subscriber service by launching the graph update and
        quote receiving threads.
        """
        # Start the graph update thread
        self.graph_thread = threading.Thread(target=self.update_graph)
        self.graph_thread.daemon = True
        self.graph_thread.start()

        # Start the update receiving thread
        self.receive_thread = threading.Thread(target=self.receive_updates)
        self.receive_thread.daemon = True
        self.receive_thread.start()

    def stop(self):
        """
        Stop the subscriber service by shutting down all threads and
        closing the socket connection.
        """
        self.running = False
        self.socket.close()
        self.graph_thread.join(timeout=1)
        self.receive_thread.join(timeout=1)

def main():
    """
    Main entry point for the subscriber service. Sets up a subscriber
    instance and runs until interrupted.
    """
    server_address = ("127.0.0.1", 50403)
    subscriber = Subscriber(server_address=server_address)
    
    try:
        subscriber.subscribe()
        subscriber.start()
        
        # Keep the main thread running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nSubscriber shutting down...")
    finally:
        subscriber.stop()

if __name__ == "__main__":
    main()