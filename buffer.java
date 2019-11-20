	public static class TextTokenizeFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {

		/**
		  * tokenlize input string and emit each word as (word, 1)
		*/

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

				// message of tweet
				StringTokenizer tokenizer = new StringTokenizer(value);

				// split the message
				while (tokenizer.hasMoreTokens()) {
					String result = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();

					if (!result.equals("")) {
						out.collect(new Tuple2<>(result, 1));
					}
				}
		}
	}

	public static class HashtagFlatMap implements FlatMapFunction<ArrayList<String>, Tuple2<String, Integer>> {

		/**
		  * tokenlize input string and emit each word as (word, 1)
		*/

		@Override
		public void flatMap(ArrayList<String> value, Collector<Tuple2<String, Integer>> out) throws Exception {

				// message of tweet
				Iterator<String> iter = value.iterator(); 

				// split the message
				while (iter.hasNext()) {
					String result = iter.next();

					if (!result.equals("")) {
						out.collect(new Tuple2<>(result, 1));
					}
				}
		}
	}
