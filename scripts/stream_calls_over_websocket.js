// Here is the code to load the SocketIO library. The backend server is: “https://api.openmhz.com”
<script src="${backend_server}/socket.io/socket.io.js"></script>
<script>
  const socket = io('${backend_server}');
</script>

//Here is the function to setup the websocket:
endSocket() {
    socket.removeAllListeners("new message");
    socket.removeAllListeners("reconnect");
  }
  setupSocket() {
    socket.on('new message', this.addCall);
    socket.on('reconnect', (attempts) => {
      console.log("Socket Reconnected after attempts: " + attempts); // true
      if (this.state.isLive)  {
        var filter = this.getFilter();
        this.startSocket(this.props.shortName, filter.type, filter.code);
      }
    })
  }
  startSocket(shortName, filterType="", filterCode="", filterName="") {
    socket.emit("start", {
      filterCode: filterCode,
      filterType: filterType,
      filterName: filterName,
      shortName: shortName
    });
  }
  stopSocket(){
    socket.emit("stop");
  }

//call it with (typestring and filtercode can be empty strings) use the shortcode of the system you want calls from:
this.startSocket(this.props.shortName, typeString, filterCode);


//then these functions play the call:
playCall(data) {
    const audio = this.audioRef.current;
    var callUrl = media_server + data.call.filename;
    if (data.call.url) {
      callUrl = data.call.url;
    }
    this.setState({
      callUrl: callUrl,
      callId: data.call._id,
      sourceIndex: 0,
      isPlaying: true
    }, () => { audio.playSource(callUrl);}); //scrollToComponent(this.currentCallRef.current);
    this.props.callActions.fetchCallInfo(this.props.shortName, data.call._id);
  }

  addCall(data) {
    const message = JSON.parse(data)
    switch (message.type) {
      case 'calls':
        const refs = this.refs.current;
        this.props.callActions.addCall(message);
        const newCall = this.props.callsById[this.props.callsAllIds[0]];
        if (!this.state.isPlaying && this.state.autoPlay) {
          this.playCall({call: newCall});
        }
        break
      default:
        break
    }
  }