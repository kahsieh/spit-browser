class Vertex {
  constructor() {
    this.sum = 0
    this.count = 0
  }

  process(data) {
    this.sum += data
    this.count += 1
    return this.sum/this.count
  }
}
