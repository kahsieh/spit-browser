class Vertex {
  constructor(sendFn) {
    this.sum = 0
    this.count = 0
    this.sendFn = sendFn
  }

  process(data) {
    this.sum += data
    this.count += 1
    this.sendFn(this.sum/this.count)
  }
}
