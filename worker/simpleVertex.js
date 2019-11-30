class Vertex {
  constructor(sendFn) {
    this.sendFn = sendFn
  }
  process(data) {
    this.sendFn(data * 2)
  }
}
