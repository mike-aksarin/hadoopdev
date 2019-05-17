trait NameDescriptor extends Serializable {
  def name: String
  override def toString = name
}
