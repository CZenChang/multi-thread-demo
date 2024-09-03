package ceizer.MutiThread;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootTest
class CompleteFutureTests {

	static ExecutorService executorService = Executors.newFixedThreadPool(8);


	@AfterAll
	public static void after() {
		executorService.shutdown();
	}

	/**
	 * whenComplete (result, throwable)
	 * BiConsumer 2入參 0出 , 僅執行 , 不回傳(不更動原CompletableFuture)
	 * 測試是否能執行兩次 依序執行
	 * 結論: 可以
	 */
	@RepeatedTest(3)
	public void test1() {

		CompletableFuture<List<String>> uCompletableFuture = doSupply();

		uCompletableFuture
				.whenCompleteAsync((t, u) ->{

					System.out.println(Thread.currentThread().getName() + "  whenComplete1");
					System.out.println(Thread.currentThread().getName() +  t.get(0));
					for (int i =0;i< 90000;i++){

					}
					System.out.println(Thread.currentThread().getName() + " done");
				})
				.whenCompleteAsync((t, u) ->{
					System.out.println(Thread.currentThread().getName() + "  whenComplete2");
				});

		try {
			Thread.sleep(1500);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

	}


	/**
	 * handle( result , Throwable)
	 * biFunction 2進參 map成另一出參
	 */
	@Test
//	@RepeatedTest(10)
	public void test2() {

		CompletableFuture<List<String>> uCompletableFuture = doSupply();

		uCompletableFuture
				.handle((t, u) ->{
					System.out.println(Thread.currentThread().getName() + "  handle1");
					System.out.println(t.get(0));
					return t.get(0);
				})
				.whenComplete((t, u) ->{
					System.out.println(Thread.currentThread().getName() + "  " + t);
				});

		try {
			Thread.sleep(1500);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}


	/**
	 * compose ,像往後連接的意思, 連接 CompletableFuture
	 * 依序執行, 可以拿到上一個result
	 * */
	@Test
//	@RepeatedTest(10)
	public void test3() {

		CompletableFuture<List<String>> uCompletableFuture = doSupply();

		uCompletableFuture.thenComposeAsync(
			list ->{
				return CompletableFuture.supplyAsync(()->{
					System.out.println(Thread.currentThread().getName() + "  compose supply start");
					System.out.println(Thread.currentThread().getName() + "  compose supply end");
					return list;
				});
			})
		.whenComplete((t, u) ->{ System.out.println(Thread.currentThread().getName() + "  " + t);});

		try {
			Thread.sleep(1500);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * combine 異步執行 兩個 completableFuture
	 * 兩個皆完成才執行 BiFunction()
	 */
//	@Test
	@RepeatedTest(3)
	public void test4() {

		CompletableFuture<List<String>> completableFuture = doSupply();

		completableFuture.thenCombineAsync(
				CompletableFuture.supplyAsync(
						() -> {
							System.out.println( Thread.currentThread().getName() + "  supply 2 start :");
							ArrayList<String> list = new ArrayList<>();
							list.add("test2");
							try {
								Thread.sleep(500);
							} catch (InterruptedException e) {
								throw new RuntimeException(e);
							}
							System.out.println( Thread.currentThread().getName() + "  supply 2 end :");
							return list;
						}, executorService
				),
				(list1, list2) -> {
					System.out.println( Thread.currentThread().getName() + "  f1 and f2 皆完成 ");
					list1.addAll(list2);
					return list1;
				}, executorService
		);

		//for doSupply() sleep 1000
		try {
			Thread.sleep(1500);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

		try {
			List<String> strings = completableFuture.get();
			System.out.println(String.join(",", strings));
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}

	}


	/**
	 * supplyAsync(要做的事supply , 指定 pool)
	 * @apiNote
	 * @return
	 * @author Ceizer
	 * @since 2024/7/11
	 */
	private CompletableFuture<List<String>> doSupply() {
		return CompletableFuture.supplyAsync(() -> {
			System.out.println(Thread.currentThread().getName() + "  supply start");
			List<String> a = new ArrayList<>();
			a.add("result");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			System.out.println(Thread.currentThread().getName() + "  supply end");
			return a;
		}, executorService);
	}

	/**
	 * acceptEither(Consumer)  处理最快完成的 Future, 其餘拋棄, 不改變原 future
	 * applyEither(Function) 处理最快完成的 Future, 其餘拋棄, 會改變原 future
	 * @apiNote
	 * @author Ceizer
	 * @since 2024/9/3
	 */
	@Test()
	public void test5() {
		CompletableFuture<String> future1 = CompletableFuture.supplyAsync(() -> {
			try {
				Thread.sleep(1000);  // 模拟延迟
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return "Result from Future 1";
		});

		CompletableFuture<String> future2 = CompletableFuture.supplyAsync(() -> {
			try {
				Thread.sleep(500);  // 模拟较短延迟
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return "Result from Future 2";
		});

		// 使用 acceptEither 处理最快完成的 Future
		future1.acceptEither(future2, result -> {
			System.out.println("Fastest result: " + result);
		});

		// 等待所有任务完成
		CompletableFuture.allOf(future1, future2).join();
	}

}
