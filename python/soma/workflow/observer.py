import weakref
import types

class Observable(object):
    
    def __init__(self):
        # A WeakKeyDictionary is one where, if the object used as the key
        # gets deleted, automatically removes that key from the
        # dictionary.  Thus, any Observers which get deleted will be
        # automatically removed from the observers dictionary, thus having
        # two effects:
        # We won't have references to zombie objects in the dictionary, and
        # We won't have zombie objects, because the reference in this
        # dictionary won't stay around, and so won't keep the deleted object
        # alive.
        self._observers = weakref.WeakKeyDictionary()

    ##
    # Add an observer to this Observable's notification list.
    # @param observer The Observer to add.
    # @param cbname The name (as a string) of the Observer's
    # method to call for an event notification, or None for the default
    # "update" method.
    # @param events The events the Observer is interested in being
    # notified about.  None means all events.

    def addObserver(self, observer, cbname=None, events=None):
        if cbname is None:
            cbname = "update"
        if events is not None and type(events) not in (types.TupleType, 
                types.ListType):
            events = (events,)
        self._observers[observer] = (cbname, events)

    ##
    # Remove an observer from this Observable's list of observers.
    # Note that this function is not strictly required, because when a
    # registered Observer is deleted, the weakref mechanism will cause
    # it to be removed from the notification list.
    # @param observer the Observer to remove.

    def removeObserver(self, observer):
        if observer in self._observers:
            del self._observers[observer]

    ##
    # Notify all currently-registered Observers.
    # Each observer must have an 'update' method, which should take
    # three parameters (in addition to self): the Observable, an event,
    # and a message.
    # This method will be
    # called if the event is one that the Observer is interested in,
    # or if event is 'None', or if the Observer is interested in all
    # events (it was registered with an event list of 'None').
    # @param event The event to notify the Observers about.  None
    # means no specific event.
    # @param msg A reference to any data object that should be passed
    # to the Observers, or None.

    def notifyObservers(self, event=None, msg=None):
        for observer, data in self._observers.items():
            #print "data is", data
            cbname, events = data
            #print "cbname is", cbname
            #print "events is", events
            #print "event is", event
            if events is None or event is None or event in events:
                #print "observer is " + repr(observer)
                cb = getattr(observer, cbname, None)
                if cb is None:
                    raise NotImplementedError, "Observer has no %s method." %cbname
                cb(self, event, msg)


##
# The abstract Observer Class
# This class is a mix-in to add Observable registration methods to
# a concrete Observer class.  It is not strictly required.
#

class Observer(object):

    ##
    # @param observable The Observable to observe.
    # @param The events this Observer is interested in being
    # notified about.  This should be a tuple or list of events.

    def __init__(self, observable=None, cbname=None, events=None):
        if (observable is not None):
            observable.addObserver(self, cbname, events)

    ##
    # Inform an Observable that you would like to be notified when
    # an interesting event occurs in the Observable.
    # @param observable The Observable this Observer would like
    # to observe.
    # @param cbname The name (as a string) of the Observer's
    # method to call for an event notification, or None for the default
    # "update" method.
    # @param events A tuple or list of events this Observer would like
    # to be notified of by the Observer, or None if it would like to
    # be notified of all events.

    def subscribeToObservable(self, observable, cbname=None, events=None):
        assert observable is not None, "Observable is None"
        observable.addObserver(self, cbname, events)

    ##
    # Inform an observable that this Observer is no longer interested in it.
    # Note that this function is not strictly required, because when a
    # registered Observer is deleted, the weakref mechanism will cause
    # it to be removed from the Observable's notification list.
    # Use this function when you want to unsubscribe an Observer
    # without deleting it.
    # @param observable The Observable that this Observer no longer wants
    # to observe.

    def unsubscribeToObservable(self, observable):
        assert observable is not None, "Observable is None"
        observable.removeObserver(self)
