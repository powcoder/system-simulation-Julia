using DataStructures
using Distributions
using StableRNGs
using Printf
using Dates

### Entity data structure for each customer
mutable struct Customer
    id::Int64
    arrival_time::Float64       # time when the customer arrives at the factory
    server::Union{Missing,Int64}# ID of server where the customer is served
    start_service_time::Float64 # time when the customer starts service
    completion_time::Float64    # time when the customer is complete
end
# generate a newly arrived customer (where paint_time and completion_time are unknown)
Customer(id::Int64, arrival_time::Float64 ) = Customer(id, arrival_time, missing, Inf, Inf)

### Events
abstract type Event end 

struct Arrival <: Event # customer arrives
    id::Int64         # a unique event id
    time::Float64     # the time of the event 
end

mutable struct Finish <: Event # a customer finishes processing at server i
    id::Int64         # a unique event id
    time::Float64     # the time of the event
    server::Int64     # ID of the server that is finishing
end

struct Null <: Event 
    id::Int64    
end

### parameter structure
struct Parameters
    seed::Int
    T::Float64
    n_queues::Int64
    mean_interarrival::Float64
    mean_server_time::Float64
    time_units::String
end
function write_parameters( output::IO, P::Parameters ) # function to writeout parameters
    T = typeof(P)
    for name in fieldnames(T)
        println( output, "# parameter: $name = $(getfield(P,name))" )
    end
end
write_parameters( P::Parameters ) = write_parameters( stdout, P )
function write_metadata( output::IO ) # function to writeout extra metadata
    (path, prog) = splitdir( @__FILE__ )
    println( output, "# file created by code in $(prog)" )
    t = now()
    println( output, "# file created on $(Dates.format(t, "yyyy-mm-dd at HH:MM:SS"))" )
end

### State
mutable struct SystemState
    time::Float64                               # the system time (simulation time)
    n_entities::Int64                           # the number of entities to have been served
    n_events::Int64                             # tracks the number of events to have occur + queued
    event_queue::PriorityQueue{Event,Float64}   # to keep track of future arravals/services
    customer_queues::Array{Queue{Customer},1}   # the system queues
    in_service::Array{Union{Customer,Nothing},1}# the customer currently in service at server i if there is one
end
function SystemState( P::Parameters ) # create an initial (empty) state
    init_time = 0.0
    init_n_entities = 0
    init_n_events = 0
    init_event_queue = PriorityQueue{Event,Float64}()
    init_customer_queues = Array{Queue{Customer},1}(undef,P.n_queues)
    for i=1:P.n_queues
        init_customer_queues[i] = Queue{Customer}()
    end
    init_in_service = Array{Union{Customer,Nothing},1}(undef,P.n_queues)
    for i=1:P.n_queues
        init_in_service[i] = nothing
    end
    return SystemState( init_time,
                        init_n_entities,
                        init_n_events,
                        init_event_queue,
                        init_customer_queues,
                        init_in_service)
end

# setup random number generators
struct RandomNGs
    rng::StableRNGs.LehmerRNG
    interarrival_time::Function
    server_time::Function
end
# constructor function to create all the pieces required
function RandomNGs( P::Parameters )
    rng = StableRNG( P.seed ) # create a new RNG with seed set to that required
    interarrival_time() = rand( rng, Exponential( P.mean_interarrival ) )  
    server_time = () -> rand( rng, Exponential( P.mean_server_time ) )
    return RandomNGs( rng, interarrival_time,  server_time )
end

# initialisation function for the simulation
function initialise( P::Parameters )
    # construct random number generators and system state
    R = RandomNGs( P )
    system = SystemState( P )

    # add an arrival at time 0.0
    t0 = 0.0
    system.n_events += 1
    enqueue!( system.event_queue, Arrival(0,t0),t0)

    return (system, R)
end

### output functions (I am using formatted output, but it could use just println)
function write_state( event_file::IO, system::SystemState, P::Parameters, event::Event, timing::AbstractString; debug_level::Int=0)
    if typeof(event) <: Finish
        type_of_event = "Finish($(event.server))"
    else
        type_of_event = typeof(event)
    end
     
    @printf(event_file,
            "%12.3f,%6d,%9s,%6s,%4d",
            system.time,
            event.id,
            type_of_event,
            timing,
            length(system.event_queue)
            )
    for i=1:P.n_queues
        @printf(event_file,",%4d,%4d", length(system.customer_queues[i]), system.in_service[i]==nothing ? 0 : 1)
    end
    @printf(event_file,"\n")
end

function write_entity_header( entity_file::IO, entity )
    T = typeof( entity )
    x = Array{Any,1}(undef, length( fieldnames(typeof(entity)) ) )
    for (i,name) in enumerate(fieldnames(T))
        tmp = getfield(entity,name)
        if isa(tmp, Array)
            x[i] = join( repeat( [name], length(tmp) ), ',' )
        else
            x[i] = name
        end
    end
    println( entity_file, join( x, ',') )
end
function write_entity( entity_file::IO, entity; debug_level::Int=0)
    T = typeof( entity )
    x = Array{Any,1}(undef,length( fieldnames(typeof(entity)) ) )
    for (i,name) in enumerate(fieldnames(T))
        tmp = getfield(entity,name)
        if isa(tmp, Array)
            x[i] = join( tmp, ',' )
        else
            x[i] = tmp
        end
    end
    println( entity_file, join( x, ',') )
end

### Update functions
function update!( system::SystemState, P::Parameters, R::RandomNGs, e::Event )
    throw( DomainError("invalid event type" ) )
end

function move_to_server!( system::SystemState, R::RandomNGs, server::Integer )
    # println("   move_to_server: $server,  $(queue_lengths( system ))   $(in_service( system )) ")
    
    # move the customer customer from a queue into construction
    system.in_service[server] = dequeue!(system.customer_queues[server]) 
    system.in_service[server].start_service_time = system.time # start service 'now'
    completion_time = system.time + R.server_time() # best current guess at service time
    
    # create a finish event for the customer
    system.n_events += 1
    finish_event = Finish( system.n_events, completion_time, server )
    enqueue!( system.event_queue, finish_event, completion_time )
    return nothing
end

function queue_lengths( system::SystemState )
    return length.( system.customer_queues )
end 
 

function in_service( system::SystemState )
    return Int.( system.in_service .!= nothing )
end

function update!( system::SystemState, P::Parameters, R::RandomNGs, event::Arrival )
    # create an arriving customer and add it to the 1st queue
    system.n_entities += 1    # new entity will enter the system
    new_customer = Customer( system.n_entities, event.time )

    # decide which queue to join based on which is shorter
    lengths = queue_lengths( system ) .+ in_service( system )
    server = argmin( lengths ) # find the shortest queue + server, and choose lowest index if there is a tie
    new_customer.server = server

    # println("  update arrival: $(queue_lengths( system ))   $(in_service( system ))    $server ")
    
    # add the customer to the appropriate queue
    enqueue!(system.customer_queues[server], new_customer)
    
    # generate next arrival and add it to the event queue
    future_arrival = Arrival(system.n_events, system.time + R.interarrival_time())
    enqueue!(system.event_queue, future_arrival, future_arrival.time)

    # if the server is available, the customer goes to service
    if system.in_service[server] == nothing
        move_to_server!( system, R, server )
    end
    return nothing
end

function update!( system::SystemState, P::Parameters, R::RandomNGs, event::Finish )
    server = event.server
    
    departing_customer = deepcopy( system.in_service[server] )
    system.in_service[server] = nothing
        
    if !isempty(system.customer_queues[server]) # if someone is waiting, move them to service
        move_to_server!( system, R, server )
    end
    
    # return the entity when it is leaving the system for good
    departing_customer.completion_time = system.time
    return departing_customer 
end

function run!( system::SystemState, P::Parameters, R::RandomNGs, fid_state::IO, fid_entities::IO; output_level::Integer=2)
    # main simulation loop
    while system.time < P.T
        if P.seed ==1 && system.time <= 1000.0
            println("$(system.time): ") # debug information for first few events whenb seed = 1
        end

        # grab the next event from the event queue
        (event, time) = dequeue_pair!(system.event_queue)
        system.time = time  # advance system time to the new arrival
        system.n_events += 1      # increase the event counter
        
        # write out event and state data before event
        if output_level>=2
            write_state( fid_state, system, P, event, "before")
        elseif output_level==1 && typeof(event) == Arrival
            write_state( fid_state, system, P, event, "before")
        end
        
        # update the system based on the next event, and spawn new events. 
        # return arrived/departed customer.
        departure = update!( system, P, R, event )
         
        # write out event and state data after event for debugging
        if output_level>=2
            write_state( fid_state, system, P, event, "after")
        end
        
        # write out entity data if it was a departure from the system
        if departure !== nothing && output_level>=2
            write_entity( fid_entities, departure )
        end
    end
    return system
end


    
